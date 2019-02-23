using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using CanalSharp.Client.ConnectorEvent;
using CanalSharp.Common.Logging;
using DotNetty.Buffers;
using DotNetty.Transport.Channels;
using Microsoft.Extensions.Logging;

namespace CanalSharp.Client.Socket
{
    public class CanalSocketHandler : ChannelHandlerAdapter
    {

        public delegate Task MessageHandler(ChannelHandlerAdapter sender, byte[] data);
        public delegate Task ExceptionHandler(ChannelHandlerAdapter sender, Exception e);

        /// <summary>
        ///     数据包头部表示有效数据长度的字节长度 即前4个字节表示有效数据长度
        /// </summary>
        private const int ProtocolHeaderLength = 4;

        private readonly PipeReader _pipeReader;
        private readonly PipeWriter _pipeWriter;
        private readonly ILogger _logger;

        public CanalSocketHandler()
        {
            var pipe = new Pipe();
            _pipeReader = pipe.Reader;
            _pipeWriter = pipe.Writer;
            _logger = CanalSharpLogManager.CreateLogger<CanalSocketHandler>();
            Task.Run(async () => await ReadPipeAsync());
        }
        
        public event MessageHandler OnMessage;
        public event ExceptionHandler OnException;

        /// <summary>
        /// 读数据
        /// </summary>
        /// <param name="context"></param>
        /// <param name="message"></param>
        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            if (message is IByteBuffer byteBuffer)
            {
                var length = byteBuffer.ReadableBytes;
                if (length <= 0) return;

                if (byteBuffer.IoBufferCount == 1)
                {
                    var bytes = byteBuffer.GetIoBuffer(byteBuffer.ReaderIndex, length);

                    _pipeWriter.WriteAsync(bytes);
                }
                else
                {
                    var bytes = new Memory<byte>(byteBuffer.Array);
                    _pipeWriter.WriteAsync(bytes);
                }
            }
        }

        /// <summary>
        /// 读取完成
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelReadComplete(IChannelHandlerContext context)=> context.Flush();

        /// <summary>
        ///  发生异常
        /// </summary>
        /// <param name="context"></param>
        /// <param name="exception"></param>
        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            _logger.LogError(exception,"Handler error.");
            OnException?.Invoke(this,exception);
        }

        public override async Task CloseAsync(IChannelHandlerContext context)
        {
            _pipeWriter.Complete();
            _pipeReader.Complete();

            await context.CloseAsync();
        }

        private async Task ReadPipeAsync()
        {
            while (true)
            {
                var result = await _pipeReader.ReadAsync();

                var buffer = result.Buffer;

                while (buffer.Length > ProtocolHeaderLength)
                {
                    //获取数据有效长度
                    var headerSequence = buffer.Slice(buffer.Start, ProtocolHeaderLength);
                    var headerBytes = headerSequence.ToArray();
                    Array.Reverse(headerBytes);
                    var dataLength = BitConverter.ToInt32(headerBytes, 0);
                    //获取数据位置
                    var dataPosition = buffer.GetPosition(ProtocolHeaderLength);
                    var dataBuffer = buffer.Slice(dataPosition);

                    if (dataBuffer.Length >= dataLength)
                    {
                        var dataSequence = dataBuffer.Slice(dataBuffer.Start, dataLength);

                        if (OnMessage != null)
                            await OnMessage(this, dataSequence.ToArray());

                        //移动指针到已经读取的数据
                        dataPosition = dataBuffer.GetPosition(dataLength);
                        buffer = dataBuffer.Slice(dataPosition);
                    }
                    else
                    {
                        break;
                    }
                }

                _pipeReader.AdvanceTo(buffer.Start);

                if (result.IsCompleted) break;
            }
        }
    }
}
using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CanalSharp.Protocol;
using DotNetty.Buffers;
using DotNetty.Transport.Channels;
using Google.Protobuf;

namespace CanalSharp.Client.Socket
{
    public class CanalSocketHandler: ChannelHandlerAdapter
    {
        public delegate void CommonHandler(object sender, EventArgs e);
        public delegate Task MessageHandler(object sender, SocketMessageEventArgs e);
        public delegate void ExceptionHandler(object sender, SocketExceptionEventArgs e);

        public event CommonHandler OnConnected;
        public event MessageHandler OnMessage;
        public event ExceptionHandler OnError;

        /// <summary>
        /// 数据包头部表示有效数据长度的字节长度 即前4个字节表示有效数据长度
        /// </summary>
        private const int ProtocolHeaderLength = 4;

        private PipeReader _pipeReader;
        private PipeWriter _pipeWriter;

        public CanalSocketHandler()
        {
            //设置一个默认实现，避免每次调用需要判断非空
            OnMessage=new MessageHandler(DefaultOnMessage);

            var pipe = new Pipe();
            _pipeReader = pipe.Reader;
            _pipeWriter = pipe.Writer;

            Task.Run(async () => await ReadPipeAsync());
        }

        public override void ChannelActive(IChannelHandlerContext context)
        {
            OnConnected?.Invoke(this, new EventArgs());
        }

        /// <summary>
        /// 读数据
        /// </summary>
        /// <param name="context"></param>
        /// <param name="message"></param>
        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            Console.WriteLine("1");
            if (message is IByteBuffer byteBuffer)
            {
                int length = byteBuffer.ReadableBytes;
                if (length <= 0)
                {
                    return;
                }

                if (byteBuffer.IoBufferCount == 1)
                {
                    ArraySegment<byte> bytes = byteBuffer.GetIoBuffer(byteBuffer.ReaderIndex, length);
                    
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
        public override void ChannelReadComplete(IChannelHandlerContext context) => context.Flush();

        /// <summary>
        /// 发生异常
        /// </summary>
        /// <param name="context"></param>
        /// <param name="exception"></param>
        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            context.CloseAsync();

            _pipeWriter.Complete();
            _pipeReader.Complete();


            OnError?.Invoke(this,new SocketExceptionEventArgs(){Exception = exception});
        }

        /// <summary>
        /// 默认实现 OnMessage 实现
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private Task DefaultOnMessage(object sender, SocketMessageEventArgs e)
        {
            return Task.CompletedTask;
        }

        private async Task ReadPipeAsync()
        {
            while (true)
            {
                ReadResult result = await _pipeReader.ReadAsync();

                ReadOnlySequence<byte> buffer = result.Buffer;

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

                        await OnMessage(this, new SocketMessageEventArgs() { Data = dataSequence.ToArray() });

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

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
    }
}
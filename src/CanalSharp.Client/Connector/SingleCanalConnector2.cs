// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using CanalSharp.Client.Socket;
using CanalSharp.Common.Logging;
using CanalSharp.Protocol;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using DotNetty.Buffers;
using System.Net;
using System.Threading.Tasks;
using CanalSharp.Protocol.Exception;
using System.Text;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace CanalSharp.Client.Connector
{
    public class SingleCanalConnector2
    {
        private readonly ILogger _logger = CanalSharpLogManager.LoggerFactory.CreateLogger<SingleCanalConnector2>();

        private readonly ClientIdentity _clientIdentity;

        private List<Compression> _supportedCompressions = new List<Compression>();
        
        /// <summary>
        /// Canal Server IP地址
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Canal Server 端口
        /// </summary>
        public int Port { get; set; }

        public string UserName { get; set; }

        public string PassWord { get; set; }

        /// <summary>
        /// milliseconds
        /// </summary>
        public int SoTimeOut { get; set; } = 60000;

        /// <summary>
        /// client 和 server 之间的空闲链接超时的时间, 默认为1小时
        /// </summary>
        public int IdleTimeOut { get; set; } = 60 * 60 * 1000;
        public int GetMesageTimeOut { get; set; } = -1;
        public int GetMesageUnit { get; set; } = 1;
        public bool GetMesageWithAck { get; set; } = false;
        public string Filter { get; set; }

        public int BitchSize { get; set; } = 1024;

        public CanalConnectState ConnectState { get; private set; }= CanalConnectState.Init;

        private IChannel _channel;

        public SingleCanalConnector2(string address, int port, string username, string password, string destination) :
            this(address, port, username, password, destination, 60000, 60 * 60 * 1000)
        {
        }

        public SingleCanalConnector2(string address, int port, string username, string password, string destination,
            int soTimeout) : this(address, port, username, password, destination, soTimeout, 60 * 60 * 1000)
        {
        }

        public SingleCanalConnector2(string address, int port, string username, string password, string destination,
            int soTimeout, int idleTimeout)
        {
            Address = address;
            Port = port;
            UserName = username;
            UserName = password;
            SoTimeOut = soTimeout;
            IdleTimeOut = idleTimeout;
            _clientIdentity = new ClientIdentity(destination,  1001);
        }

        /*private void DoConnect()
        {
            _tcpClient = new TcpClient(Address, Port);
            _channelNetworkStream = _tcpClient.GetStream();
            var p = Packet.Parser.ParseFrom(ReadNextPacket());
            if (p != null)
            {
                if (p.Version != 1)
                {
                    throw new CanalClientException("unsupported version at this client.");
                }

                if (p.Type != PacketType.Handshake)
                {
                    throw new CanalClientException("expect handshake but found other type.");
                }

                var handshake = Handshake.Parser.ParseFrom(p.Body);
                _supportedCompressions.Add(handshake.SupportedCompressions);

                var ca = new ClientAuth()
                {
                    Username = UserName ?? "",
                    Password = ByteString.CopyFromUtf8(PassWord ?? ""),
                    NetReadTimeout = IdleTimeOut,
                    NetWriteTimeout = IdleTimeOut
                };

                var packArray = new Packet()
                {
                    Type = PacketType.Clientauthentication,
                    Body = ca.ToByteString()
                }.ToByteArray();

                WriteWithHeader(packArray);

                var packet = Packet.Parser.ParseFrom(ReadNextPacket());
                if (packet.Type != PacketType.Ack)
                {
                    throw new CanalClientException("unexpected packet type when ack is expected");
                }

                var ackBody = Protocol.Ack.Parser.ParseFrom(p.Body);
                if (ackBody.ErrorCode > 0)
                {
                    throw new CanalClientException("something goes wrong when doing authentication:" +
                                                   ackBody.ErrorMessage);
                }

                _connected = _tcpClient.Connected;
                _logger.LogDebug($"Canal connect success. IP: {Address}, Port: {Port}");
            }
        }*/

        public async Task ConnectAsync()
        {
            var handler = new CanalSocketHandler();
            handler.OnMessage += ProcessMessage;

            var bootstrap = new Bootstrap();
            bootstrap
                .Group(new SingleThreadEventLoop())
                .Channel<TcpSocketChannel>()
                .Option(ChannelOption.SoTimeout, SoTimeOut)
                .Handler(new ActionChannelInitializer<ISocketChannel>(channel =>
                {
                    IChannelPipeline pipeline = channel.Pipeline;
                    pipeline.AddLast("Canal", handler);
                }));

            _channel = await bootstrap.ConnectAsync(new IPEndPoint(IPAddress.Parse(Address), Port));
        }

        private async Task ProcessMessage(object sender, MessageEventArgs e)
        {
            await SendHandshake();
            await SendSubscribe();
//            try
//            {
//
//                switch (ConnectState)
//                {
//                    case CanalConnectState.Init:
//                        await ProcessInitAsync(e.Data);
//                        break;
//                    case CanalConnectState.Handshake:
//                        await ProcessHandshakeAsync(e.Data);
//                        break;
//                    case CanalConnectState.Connected:
//                        await ProcessSubscribeAsync(e.Data);
//                        break;
//                    default:
//                        throw new CanalClientException("Unexpected Packets.");
//                }
//            }
//            catch (Exception exception)
//            {
//                Console.WriteLine(exception);
//                throw;
//            }
        }

        /// <summary>
        /// Setp 1：确认初始化包，发送握手包 Init-> Handshake
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private async Task ProcessInitAsync(byte[] data)
        {
            var p = Packet.Parser.ParseFrom(data);
            if (p != null)
            {
                if (p.Version != 1)
                {
                    throw new CanalClientException("unsupported version at this client.");
                }

                if (p.Type != PacketType.Handshake)
                {
                    throw new CanalClientException("expect handshake but found other type.");
                }
                var handshake = Handshake.Parser.ParseFrom(p.Body);
                _supportedCompressions.Add(handshake.SupportedCompressions);

                var ca = new ClientAuth()
                {
                    Username = UserName ?? "",
                    Password = ByteString.CopyFromUtf8(PassWord ?? ""),
                    NetReadTimeout = IdleTimeOut,
                    NetWriteTimeout = IdleTimeOut
                };

                var packArray = new Packet()
                {
                    Type = PacketType.Clientauthentication,
                    Body = ca.ToByteString()
                }.ToByteArray();
                await SendDataAsync(packArray);

                //                var ackBody = Protocol.Ack.Parser.ParseFrom(p.Body);
                //                if (ackBody.ErrorCode > 0)
                //                {
                //                    throw new CanalClientException("something goes wrong when doing authentication:" +
                //                                                   ackBody.ErrorMessage);
                //                }
                //
                //
                //                var handshake = Handshake.Parser.ParseFrom(p.Body);
                //                _supportedCompressions.Add(handshake.SupportedCompressions);
                //
                //                await SendHandshake();
                //                ConnectState = CanalConnectState.Handshake;
            }
        }

        /// <summary>
        /// Setp 2：确认握手结果包，发送订阅包 Handshake -> Connected
        /// </summary>
        private async Task ProcessHandshakeAsync(byte[] data)
        {
            var packet = Packet.Parser.ParseFrom(data);
            if (packet.Type != PacketType.Ack)
            {
                throw new CanalClientException("unexpected packet type when ack is expected");
            }

            ConnectState = CanalConnectState.Connected;
            await SendSubscribe();
        }

        /// <summary>
        /// Setp 3：确认订阅包，发送获取数据包 Connected -> Subscribe
        /// </summary>
        private async Task ProcessSubscribeAsync(byte[] data)
        {
            var p = Packet.Parser.ParseFrom(data);
            var ack = Ack.Parser.ParseFrom(p.Body);
            if (ack.ErrorCode > 0)
            {
                throw new CanalClientException($"failed to subscribe with reason: {ack.ErrorMessage}");
            }

            _clientIdentity.Filter = Filter;

            ConnectState = CanalConnectState.Subscribe;

            await SendGetMessage();
        }

        /// <summary>
        /// 处理收到的数据
        /// </summary>
        private async Task ProcessMessage(byte[] data)
        {
            var packet = Packet.Parser.ParseFrom(data);
            if (packet.Type != PacketType.Ack)
            {
                throw new CanalClientException("unexpected packet type when ack is expected");
            }

            ConnectState = CanalConnectState.Connected;
            await SendGetMessage();
        }

        #region 发送数据包

        /// <summary>
        /// 发送握手包
        /// </summary>
        /// <returns></returns>
        private async Task SendHandshake()
        {

            var ca = new ClientAuth()
            {
                Username = UserName ?? "",
                Password = ByteString.CopyFromUtf8(PassWord ?? ""),
                NetReadTimeout = IdleTimeOut,
                NetWriteTimeout = IdleTimeOut
            };

            var packArray = new Packet()
            {
                Type = PacketType.Clientauthentication,
                Body = ca.ToByteString()
            }.ToByteArray();

            await SendDataAsync(packArray);
        }

        /// <summary>
        /// 发送订阅包
        /// </summary>
        /// <returns></returns>
        private async Task SendSubscribe()
        {
            var sub = new Sub()
            {
                Destination = _clientIdentity.Destination,
                ClientId = _clientIdentity.ClientId.ToString(),
                Filter = string.IsNullOrEmpty(Filter) ? ".*\\..*" : Filter
            };
            var pack = new Packet()
            {
                Type = PacketType.Subscription,
                Body = sub.ToByteString()
            }.ToByteArray();

            await SendDataAsync(pack);
        }

        /// <summary>
        /// 发送获取消息数据包
        /// </summary>
        /// <returns></returns>
        private async Task SendGetMessage()
        {
            var get = new Get()
            {
                AutoAck = GetMesageWithAck,
                Destination = _clientIdentity.Destination,
                ClientId = _clientIdentity.ClientId.ToString(),
                FetchSize = BitchSize,
                Timeout = GetMesageTimeOut,
                Unit = GetMesageUnit
            };
            var packet = new Packet()
            {
                Type = PacketType.Get,
                Body = get.ToByteString()
            }.ToByteArray();
            await SendDataAsync(packet);
        }

        /// <summary>
        /// 发送数据包
        /// </summary>
        /// <param name="body"></param>
        private async Task SendDataAsync(byte[] body)
        {
            try
            {

                var headerBytes = BitConverter.GetBytes(body.Length);
//                Array.Reverse(headerBytes);
                var msg = Unpooled.Buffer(body.Length+headerBytes.Length);
                msg.WriteBytes(headerBytes);
                msg.WriteBytes(body);
                await _channel.WriteAndFlushAsync(msg);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        #endregion

    }
}
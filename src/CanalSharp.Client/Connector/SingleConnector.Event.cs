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
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CanalSharp.Client.ConnectorEvent;
using CanalSharp.Client.Socket;
using CanalSharp.Common.Exceptions;
using CanalSharp.Common.Logging;
using CanalSharp.Protocol;
using DotNetty.Buffers;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace CanalSharp.Client.Connector
{
    /// <summary>
    /// Single means does not support clustering
    /// <para></para>
    /// This Connector use DotNetty and System.IO.Pipelines and Event Subscription Design.Recommend !
    /// </summary>
    public class EventSingleConnector
    {

        private readonly ILogger _logger;
        private readonly ClientIdentity _clientIdentity;
        private readonly SingleConnectorOptions _options;
        private readonly List<Compression> _supportedCompressions;
        private readonly MultithreadEventLoopGroup _nettyGroup;


        private IChannel _channel;
        private int _errorCount=0;
        private int _emptyCount=0;
        private const int MaxErrorCount=50;
        private const int MaxEmptyCount=200;
        private const int EmptySleep=1000;
        public ConnectState ConnectState { get; private set; }

        public delegate void MessageHandler(EventSingleConnector sender, Message data);
        public delegate void CommonHandler(EventSingleConnector sender);

        public event MessageHandler OnMessage;
        public event CommonHandler OnReady;

        //Exit application signal
        private bool _stopRunning = false;
        private bool _handling = false;

        /// <summary>
        /// Create instance.
        /// </summary>
        /// <param name="address">Canal Server Ip Address</param>
        /// <param name="port">Canal Server Ip Port</param>
        /// <param name="destination">Canal Server Destination</param>
        /// <param name="filter">Subscription filtering</param>
        public EventSingleConnector(string address, int port, string destination, string filter) : this(
            new SingleConnectorOptions() {Address = address, Port = port, Destination = destination, Filter = filter})
        {
        }

        public EventSingleConnector(SingleConnectorOptions options)
        {
            _options = options;
            _nettyGroup = new MultithreadEventLoopGroup();
            _clientIdentity = new ClientIdentity(options.Destination, _options.ClientId,options.Filter);
            _logger = CanalSharpLogManager.CreateLogger<EventSingleConnector>();
            _supportedCompressions = new List<Compression>(){Compression.None};
            ConnectState = ConnectState.Init;
        }

        /// <summary>
        /// Connect to Canal Server
        /// </summary>
        /// <returns></returns>
        public async Task ConnectAsync()
        {
            if (ConnectState > ConnectState.Init)
            {
                return;
            }

            try
            {
                var handler = new CanalSocketHandler();
                handler.OnMessage += ProcessMessage;
                handler.OnException += (sender, ex) => {
                    _logger.LogError(ex, "Socke error.");
                    return Task.CompletedTask;
                };

                var bootstrap = new Bootstrap();
                bootstrap
                    .Group(_nettyGroup)
                    .Channel<TcpSocketChannel>()
                    .Option(ChannelOption.SoTimeout, _options.SoTimeOut)
                    .Option(ChannelOption.ConnectTimeout, TimeSpan.FromMilliseconds(_options.ConnectTimeout))
                    .Handler(new ActionChannelInitializer<ISocketChannel>(channel =>
                    {
                        var pipeline = channel.Pipeline;
                        pipeline.AddLast("CanalSharp", handler);
                    }));

                _logger.LogInformation($"Connecting to [{_options.Address}:{_options.Port}/{_options.Destination}]");
                _channel = await bootstrap.ConnectAsync(new IPEndPoint(IPAddress.Parse(_options.Address), _options.Port));
            }
            catch (Exception e)
            {
                await _nettyGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
                throw new CanalConnectException("Failed to establish socket connection.",e);
            }
        }

        public async Task CloseAsync()
        {
            if (ConnectState == ConnectState.Init)
            {
                return;
            }

            //set exit application signal
            _stopRunning = true;
            //wait for working
            while (_handling)
            {
                await Task.Delay(100);
            }

            if (_channel != null)
            {
                await _channel.DisconnectAsync();
            }

            if (_nettyGroup != null)
                await _nettyGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));

            ConnectState = ConnectState.Init;

            _logger.LogInformation("Close the connection successfully");
            _stopRunning = false;
            _handling = false;
        }

        /// <summary>
        /// 订阅过滤
        /// <para></para>
        /// 规则：
        /// <para></para>
        /// 允许所有数据： .*\\..*
        /// <para></para>
        /// 允许某个库数据： 库名\\..*
        /// <para></para>
        /// 允许某些表： 库名.表名,库名.表名
        /// <para></para>
        /// 默认允许所有数据
        /// </summary>
        public async Task ChangeSubscribeAsync(string filter)
        {
            if (string.IsNullOrEmpty(filter)||ConnectState<ConnectState.Subscribe)
            {
                return;
            }

            _options.Filter = filter;
            _clientIdentity.Filter = filter;
            await SendSubscribeAsync();
        }


        public async Task AckAsync(long batchId)
        {
            if (batchId<=0 || ConnectState <= ConnectState.Subscribe)
            {
                return;
            }

            var ca = new ClientAck
            {
                Destination = _options.Destination,
                ClientId = _options.ClientId.ToString(),
                BatchId = batchId
            };

            var pack = new Packet
            {
                Type = PacketType.Clientack,
                Body = ca.ToByteString()
            }.ToByteArray();

            await SendPacketAsync(pack);

            _logger.LogDebug($"Ack {batchId} success.");
        }

        public async Task RollbackAsync(long batchId)
        {
            if (batchId <= 0 || ConnectState <= ConnectState.Subscribe)
            {
                return;
            }

            var ca = new ClientAck
            {
                Destination = _options.Destination,
                ClientId = _options.ClientId.ToString(),
                BatchId = batchId
            };

            var pack = new Packet
            {
                Type = PacketType.Clientrollback,
                Body = ca.ToByteString()
            }.ToByteArray();
            await SendPacketAsync(pack);

            _logger.LogDebug($"Rollback {batchId} success.");
        }

        #region Process packets

        private async Task ProcessMessage(ChannelHandlerAdapter sender,byte[] data)
        {
            try
            {
                _handling = true;
                switch (ConnectState)
                {
                    case ConnectState.Init:
                        await ProcessInitAsync(data);
                        break;
                    case ConnectState.Handshake:
                        await ProcessHandshakeAsync(data);
                        break;
                    case ConnectState.Connected:
                        await ProcessSubscribeAsync(data);
                        break;
                    case ConnectState.Subscribe:
                        await ProcessReceiveMessage(data);
                        break;
                    default:
                        throw new CanalProtocolException("Unexpected Packets.");
                }

                _errorCount = 0;
            }
            catch (Exception ex)
            {
                _errorCount++;
                if (ex is CanalConnectException)
                {
                    _logger.LogError(ex,
                        "Exceptions occurred during connection and the program could not continue to run.");
                    await CloseAsync();
                    throw;
                }

                if (ex is ClosedChannelException)
                {
                    _logger.LogError(ex, "Connection interruption due to exceptions.");
                    await CloseAsync();
                    throw;
                }
                else
                {
                    _logger.LogError(ex, "An error occurred while processing the data package.");

                    if (_errorCount >= MaxErrorCount)
                    {
                        await CloseAsync();
                        throw new CanalException(
                            $"The number of errors has reached the threshold {MaxErrorCount} and get data is stopped. Application Exit!");
                    }
                    else if (ConnectState == ConnectState.Subscribe)
                    {
                        await SendGetMessageAsync();
                    }
                }
            }
            finally
            {
                _handling = false;
            }
        }

        /// <summary>
        /// Setp 1：Confirm the initialization packet and send the handshake packet
        /// <para></para>
        /// Init-> Handshake
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private async Task ProcessInitAsync(byte[] data)
        {
            var p = Packet.Parser.ParseFrom(data);
            if (p != null)
            {
                if (p.Version != 1) throw new CanalConnectException("Unsupported version at this client.");

                if (p.Type != PacketType.Handshake) throw new CanalConnectException("Expect handshake but found other type.");
            }

            var ackBody = Ack.Parser.ParseFrom(p.Body);
            if (ackBody.ErrorCode > 0) throw new CanalConnectException("Something goes wrong when doing authentication: " +
                                                                       ackBody.ErrorMessage);

            var handshake = Handshake.Parser.ParseFrom(p.Body);
            _supportedCompressions.Add(handshake.SupportedCompressions);

            ConnectState = ConnectState.Handshake;

            await SendHandshakeAsync();
        }

        /// <summary>
        /// Setp 2：Confirm handshake result packet and send subscription packet
        /// <para></para>
        /// Handshake -> Connected
        /// </summary>
        private async Task ProcessHandshakeAsync(byte[] data)
        {
            var packet = Packet.Parser.ParseFrom(data);
            if (packet.Type != PacketType.Ack) throw new CanalConnectException("Unexpected packet type when ack is expected.");

            ConnectState = ConnectState.Connected;

            await SendSubscribeAsync();
        }

        /// <summary>
        /// Setp 3：Confirm subscription packet and send receive message packet
        ///  <para></para>
        /// Connected -> Subscribe
        /// </summary>
        private async Task ProcessSubscribeAsync(byte[] data)
        {
            var p = Packet.Parser.ParseFrom(data);
            var ack = Ack.Parser.ParseFrom(p.Body);
            if (ack.ErrorCode > 0) throw new CanalConnectException($"Failed to subscribe with reason: {ack.ErrorMessage}");

            ConnectState = ConnectState.Subscribe;
            OnReady?.Invoke(this);

            _logger.LogInformation($"Subscribe to [{_options.Filter}]");
            await SendGetMessageAsync();
        }

        /// <summary>
        /// Setp 4：Processing Canal binlog packets
        /// </summary>
        private async Task ProcessReceiveMessage(byte[] data)
        {
            var p = Packet.Parser.ParseFrom(data);
            switch (p.Type)
            {
                case PacketType.Messages:
                {
                    if (!_supportedCompressions.Contains(p.Compression))
                        throw new CanalProtocolException($"Compression '{p.Compression}' is not supported in this connector.");

                    var messages = Messages.Parser.ParseFrom(p.Body);
                    var result = new Message(messages.BatchId);

                    if (_options.LazyParseEntry)
                        result.RawEntries = messages.Messages_.ToList();
                    else
                        foreach (var byteString in messages.Messages_)
                            result.Entries.Add(Entry.Parser.ParseFrom(byteString));

                    if (result.Id != -1 || result.Entries.Count > 0)
                    {
                        _emptyCount = 0;
                        OnMessage?.Invoke(this, result);
                    }
                    else
                    {
                        if (_emptyCount >= MaxEmptyCount)
                        {
                            _logger.LogDebug($"No data has been obtained in the last {MaxEmptyCount} requests, the program will enter a low consumption mode.");
                            await Task.Delay(EmptySleep);
                        }
                        else
                        {
                            _emptyCount++;
                        }

                    }

                    break;
                }
                case PacketType.Ack:
                {
                    var ack = Ack.Parser.ParseFrom(p.Body);
                    if (ack.ErrorCode > 0) throw new CanalProtocolException($"something goes wrong with reason:{ack.ErrorMessage}");
                    break;
                }
                default:
                {
                    throw new CanalProtocolException($"Unexpected packet type: {p.Type}");
                }
            }

            await SendGetMessageAsync();
        }

        #endregion

        #region Send packets

        /// <summary>
        /// Send handshake packet
        /// </summary>
        /// <returns></returns>
        private async Task SendHandshakeAsync()
        {

            var ca = new ClientAuth
            {
                Username = _options.Username ?? "",
                Password = ByteString.CopyFromUtf8(_options.Password ?? ""),
                NetReadTimeout = _options.SoTimeOut,
                NetWriteTimeout = _options.SoTimeOut
            };

            var packet = new Packet
            {
                Type = PacketType.Clientauthentication,
                Body = ca.ToByteString()
            }.ToByteArray();

            await SendPacketAsync(packet);
        }

        /// <summary>
        /// Send subscribe packet
        /// </summary>
        /// <returns></returns>
        private async Task SendSubscribeAsync()
        {
            var sub = new Sub
            {
                Destination = _options.Destination,
                ClientId = _options.ClientId.ToString(),
                Filter = string.IsNullOrEmpty(_options.Filter) ? ".*\\..*" : _options.Filter
            };
            var pack = new Packet
            {
                Type = PacketType.Subscription,
                Body = sub.ToByteString()
            }.ToByteArray();
            _options.Filter = sub.Filter;
            await SendPacketAsync(pack);
        }

        /// <summary>
        /// Send get message packet
        /// </summary>
        /// <returns></returns>
        private async Task SendGetMessageAsync()
        {
            var get = new Get
            {
                AutoAck = _options.AutoAck,
                Destination = _options.Destination,
                ClientId = _options.ClientId.ToString(),
                FetchSize = _options.BitchSize,
                Timeout = _options.ReceiveMesageTimeOut,
                Unit = _options.ReceiveMesageUnit
            };
            var packet = new Packet
            {
                Type = PacketType.Get,
                Body = get.ToByteString()
            }.ToByteArray();
            await SendPacketAsync(packet);
        }

        /// <summary>
        /// Send packets
        /// </summary>
        /// <param name="body"></param>
        private async Task SendPacketAsync(byte[] body)
        {
            if (_stopRunning)
            {
                return;
            }

            var headerBytes = BitConverter.GetBytes(body.Length);
            Array.Reverse(headerBytes);
            var msg = Unpooled.Buffer(body.Length + headerBytes.Length);
            msg.WriteBytes(headerBytes);
            msg.WriteBytes(body);
            await _channel.WriteAndFlushAsync(msg);
        }

        #endregion
    }
}
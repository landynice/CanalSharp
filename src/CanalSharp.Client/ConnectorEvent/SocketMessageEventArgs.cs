using System;

namespace CanalSharp.Client.ConnectorEvent
{
    public class SocketMessageEventArgs : EventArgs
    {
        public byte[] Data { get; set; }
    }
}
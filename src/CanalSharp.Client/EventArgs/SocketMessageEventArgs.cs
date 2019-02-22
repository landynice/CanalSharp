using System;

namespace CanalSharp.Client.Socket
{
    public class SocketMessageEventArgs : EventArgs
    {
        public byte[] Data { get; set; }
    }
}
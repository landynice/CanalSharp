using System;

namespace CanalSharp.Client.Socket
{
    public class MessageEventArgs : EventArgs
    {
        public byte[] Data { get; set; }
    }
}
using System;
using CanalSharp.Protocol;

namespace CanalSharp.Client.Connector
{
    public class CanalMessageEventArgs : EventArgs
    {
        public Message Data { get; set; }
    }
}
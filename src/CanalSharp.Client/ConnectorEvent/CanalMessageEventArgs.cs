using System;
using CanalSharp.Protocol;

namespace CanalSharp.Client.ConnectorEvent
{
    public class CanalMessageEventArgs : EventArgs
    {
        public Message Data { get; set; }
    }
}
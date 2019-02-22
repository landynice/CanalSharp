using System;

namespace CanalSharp.Client.Socket
{
    public class SocketExceptionEventArgs:EventArgs
    {
        public Exception Exception { get; set; }
    }
}
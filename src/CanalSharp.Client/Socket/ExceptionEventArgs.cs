using System;

namespace CanalSharp.Client.Socket
{
    public class ExceptionEventArgs:EventArgs
    {
        public Exception Exception { get; set; }
    }
}
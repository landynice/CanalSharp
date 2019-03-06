using System.Collections.Concurrent;

namespace CanalSharp.Client.Connector
{
    public class ConnectorOperationQueue
    {
        public ConcurrentQueue<string> _queue;

        public ConnectorOperationQueue()
        {
            _queue = new BlockingCollection<string>();
        }
    }
}
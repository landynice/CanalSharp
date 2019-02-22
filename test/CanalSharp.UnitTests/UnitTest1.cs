using System;
using System.Threading;
using CanalSharp.Client;
using CanalSharp.Client.Connector;
using Xunit;

namespace CanalSharp.UnitTests
{
    public class UnitTest1
    {
        private static readonly object _lock = new object();
        [Fact]
        public void Test1()
        {
            string destination = "example";
            IConnector connector = ConnectorFactory.CreateSingleConnector("127.0.0.1", 11111, destination, "", "");
            connector.Connect();
            Console.Read();

            Mutex a=new Mutex(false);
            //lock (_lock)
            //{
            //    new Thread(Test).Start();
            //    Monitor.Wait(_lock);
            //    Console.WriteLine(aa);
            //    Console.Read();
            //}

        }

        [Fact]
        public void Test()
        {
            lock (_lock)
            {
                Thread.Sleep(500);
                Monitor.Pulse(_lock);
            }
            
        }
    }
}

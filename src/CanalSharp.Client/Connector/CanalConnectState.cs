namespace CanalSharp.Client.Connector
{
    public enum CanalConnectState
    {
        Init = 0,
        Handshake,
        Connected,
        Subscribe,
    }
}
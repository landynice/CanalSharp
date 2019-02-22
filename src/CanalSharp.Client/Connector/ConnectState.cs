namespace CanalSharp.Client.Connector
{
    public enum ConnectState
    {
        /// <summary>
        /// No initialization
        /// </summary>
        Init = 0,
        /// <summary>
        /// Shaking hands
        /// </summary>
        Handshake,
        /// <summary>
        /// Handshake successfully and establish connections
        /// </summary>
        Connected,
        /// <summary>
        /// Canal Server data is available
        /// </summary>
        Subscribe
    }
}
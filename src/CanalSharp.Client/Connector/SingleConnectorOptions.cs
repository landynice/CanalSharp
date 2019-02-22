// #region File Annotation
// 
// Author：Zhiqiang Li
// 
// FileName：SingleConnectorOptions.cs
// 
// Project：CanalSharp.Client
// 
// CreateDate：2019/02/22
// 
// Note: The reference to this document code must not delete this note, and indicate the source!
// 
// #endregion

namespace CanalSharp.Client.Connector
{
    public class SingleConnectorOptions
    {
        /// <summary>
        /// Canal Server IP地址
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Canal Server 端口
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Canal Server 认证用户名
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// Canal Server 认证密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// CanalSharp Client Id
        /// <para></para>
        /// Default: 1001
        /// </summary>
        public short ClientId { get; set; } = 1001;

        /// <summary>
        /// Socket 读数据超时时间
        /// <para></para>
        /// 单位：毫秒
        /// <para></para>
        /// 默认值：60000 即1分钟
        /// </summary>
        public int SoTimeOut { get; set; } = 60000;

        /// <summary>
        /// Socket 连接超时
        /// <para></para>
        /// 单位：毫秒
        /// <para></para>
        /// 默认值：60000 即1分钟
        /// </summary>
        public int ConnectTimeout { get; set; } = 60000;

        /// <summary>
        /// CanalSharp 和 Canal Server 之间的空闲链接超时的时间
        /// <para></para>
        /// 单位：毫秒
        /// <para></para>
        /// 默认值：60 * 60 * 1000 即1小时
        /// </summary>
        public int IdleTimeOut { get; set; } = 60 * 60 * 1000;

        /// <summary>
        /// 获取 Canal 数据超时时间
        /// <para></para>
        /// 默认值：-1 即不受超时限制
        /// </summary>
        public int ReceiveMesageTimeOut { get; set; } = -1;

        /// <summary>
        /// 获取 Canal 数据单位
        /// <para></para>
        /// 默认值：1
        /// </summary>
        public int ReceiveMesageUnit { get; set; } = 1;

        /// <summary>
        /// 是否自动发送回执
        /// <para></para>
        /// 发送了回执才表示当前记录消费成功，Canal Server 才会更新消费进度
        /// <para></para>
        /// True 自动发送回执，False 需要自己控制发送回执
        /// <para></para>
        /// 默认值：True
        /// </summary>
        public bool AutoAck { get; set; } = true;


        /// <summary>
        /// 是否延后解析解析Entry对象, 如果考虑最大化性能可以延后解析
        /// <para></para>
        /// 默认值：False
        /// </summary>
        public bool LazyParseEntry { get; set; } = false;

        /// <summary>
        /// Canal Destination 用于隔离
        /// <para></para>
        /// 默认值：example
        /// </summary>
        public string Destination { get; set; } = "example";

        /// <summary>
        /// 订阅过滤
        /// <para></para>
        /// 规则：
        /// <para></para>
        /// 允许所有数据： .*\\..*
        /// <para></para>
        /// 允许某个库数据： 库名\\..*
        /// <para></para>
        /// 允许某些表： 库名.表名,库名.表名
        /// <para></para>
        /// 默认允许所有数据
        /// </summary>
        public string Filter { get; set; } = ".*\\..*";

        /// <summary>
        /// 获取数据批次大小
        /// <para></para>
        /// 单位：字节
        /// <para></para>
        /// 默认值：2048
        /// </summary>
        public int BitchSize { get; set; } = 2048;
    }
}
// #region File Annotation
// 
// Author：Zhiqiang Li
// 
// FileName：CanalConnectException.cs
// 
// Project：CanalSharp.Protocol
// 
// CreateDate：2019/02/22
// 
// Note: The reference to this document code must not delete this note, and indicate the source!
// 
// #endregion

using System;

namespace CanalSharp.Common.Exceptions
{
    public class CanalConnectException : Exception
    {
        public CanalConnectException(string message) : base(message)
        {
        }

        public CanalConnectException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
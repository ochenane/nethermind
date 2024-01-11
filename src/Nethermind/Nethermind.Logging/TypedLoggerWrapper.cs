// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;

namespace Nethermind.Logging;

/// <summary>
/// Just a wrapper around non type ILogger
/// </summary>
/// <typeparam name="T"></typeparam>
public class TypedLoggerWrapper<T> : ILogger<T>
{
    private ILogger _loggerImplementation;

    public TypedLoggerWrapper(ILogger logger)
    {
        _loggerImplementation = logger;
    }

    public void Info(string text)
    {
        _loggerImplementation.Info(text);
    }

    public void Warn(string text)
    {
        _loggerImplementation.Warn(text);
    }

    public void Debug(string text)
    {
        _loggerImplementation.Debug(text);
    }

    public void Trace(string text)
    {
        _loggerImplementation.Trace(text);
    }

    public void Error(string text, Exception ex = null)
    {
        _loggerImplementation.Error(text, ex);
    }

    public bool IsInfo => _loggerImplementation.IsInfo;

    public bool IsWarn => _loggerImplementation.IsWarn;

    public bool IsDebug => _loggerImplementation.IsDebug;

    public bool IsTrace => _loggerImplementation.IsTrace;

    public bool IsError => _loggerImplementation.IsError;
}

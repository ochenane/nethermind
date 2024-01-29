// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Reflection;
using Autofac;
using Autofac.Builder;
using Autofac.Core;
using Nethermind.Db;

namespace Nethermind.Runner.Modules;

public static class RegistrationBuilderExtensions
{
    public static IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle>
        WithDb<TLimit, TReflectionActivatorData, TStyle>(
            this IRegistrationBuilder<TLimit, TReflectionActivatorData, TStyle> registration, string dbName)
        where TReflectionActivatorData : ReflectionActivatorData
    {
        return registration.WithParameter(new DbNameParameter(dbName));
    }

    private class DbNameParameter : Parameter
    {
        private readonly string _dbName;

        public DbNameParameter(string dbName)
        {
            _dbName = dbName;
        }

        public override bool CanSupplyValue(ParameterInfo pi, IComponentContext context, out Func<object?>? valueProvider)
        {
            if (pi.ParameterType != typeof(IDb))
            {
                valueProvider = null;
                return false;
            }

            valueProvider = () => context.ResolveNamed<IDb>(_dbName);
            return true;
        }
    }
}


public class ParameterFromConfig<T> : Parameter
{
    private readonly string _parameterName;
    private readonly Func<T, object> _accessor;

    public ParameterFromConfig(string parameterName, Func<T, object> accessor)
    {
        _parameterName = parameterName;
        _accessor = accessor;
    }

    public override bool CanSupplyValue(ParameterInfo pi, IComponentContext context, out Func<object?>? valueProvider)
    {
        if (pi.Name != _parameterName)
        {
            valueProvider = null;
            return false;
        }

        valueProvider = () => _accessor(context.Resolve<T>());
        return true;
    }
}

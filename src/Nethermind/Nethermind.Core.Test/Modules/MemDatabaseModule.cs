// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac;
using Nethermind.Config;
using Nethermind.Db;
using Nethermind.Runner.Modules;
using NSubstitute;

namespace Nethermind.Core.Test.Modules;

public class MemDatabaseModule: Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder.RegisterModule(new DatabaseModule(Substitute.For<IConfigProvider>()));
        builder.RegisterType<MemDbFactory>().As<IDbFactory>();
    }

    public static IDbProvider CreateDbProvider()
    {
        ContainerBuilder builder = new ContainerBuilder();
        builder.RegisterModule(new MemDatabaseModule());
        return builder.Build().Resolve<IDbProvider>();
    }
}

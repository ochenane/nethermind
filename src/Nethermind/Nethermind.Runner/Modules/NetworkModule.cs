// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac;
using Nethermind.Core.Timers;
using Nethermind.Evm.Tracing.GethStyle.JavaScript;
using Nethermind.Logging;
using Nethermind.Network.Config;
using Nethermind.Stats;

namespace Nethermind.Runner.Modules;

public class NetworkModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        // Some dependency issue with INetworkConfig prevented automatic constructor injection here
        builder.Register<ITimerFactory, INetworkConfig, ILogManager, NodeStatsManager>(
                (tf, nc, lm) => new NodeStatsManager(tf, lm, nc.MaxCandidatePeerCount))
            .As<INodeStatsManager>();
    }
}

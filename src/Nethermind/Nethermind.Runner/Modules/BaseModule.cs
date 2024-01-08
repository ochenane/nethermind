// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Autofac;
using Autofac.Features.ResolveAnything;
using Nethermind.Api;
using Nethermind.Config;
using Nethermind.Consensus;
using Nethermind.Core.Specs;
using Nethermind.Core.Timers;
using Nethermind.Crypto;
using Nethermind.Logging;
using Nethermind.Network.Config;
using Nethermind.Serialization.Json;
using Nethermind.Specs.ChainSpecStyle;

namespace Nethermind.Runner.Modules;

public class BaseModule : Module
{
    private readonly IConfigProvider _configProvider;
    private readonly IProcessExitSource _processExitSource;
    private readonly ChainSpec _chainSpec;
    private readonly ILogManager _logManager;
    private readonly IJsonSerializer _jsonSerializer;

    public BaseModule(
        IConfigProvider configProvider,
        IProcessExitSource processExitSource,
        ChainSpec chainSpec,
        IJsonSerializer jsonSerializer,
        ILogManager logManager
    )
    {
        _configProvider = configProvider;
        _processExitSource = processExitSource;
        _chainSpec = chainSpec;
        _jsonSerializer = jsonSerializer;
        _logManager = logManager;

        SetLoggerVariables(_chainSpec, _logManager);
    }

    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder.RegisterSource(new AnyConcreteTypeNotAlreadyRegisteredSource());
        builder.RegisterInstance(_configProvider);
        builder.RegisterInstance(_processExitSource);
        builder.RegisterInstance(_chainSpec);
        builder.RegisterInstance(_jsonSerializer);
        builder.RegisterInstance(_logManager);

        builder.RegisterType<ChainSpecBasedSpecProvider>()
            .As<ISpecProvider>()
            .SingleInstance();

        builder.RegisterType<CryptoRandom>()
            .As<ICryptoRandom>()
            .SingleInstance();

        builder.RegisterType<FollowOtherMiners>()
            .As<IGasLimitCalculator>()
            .SingleInstance();

        builder.RegisterType<NethermindApi>()
            .As<INethermindApi>()
            .SingleInstance();

        builder.RegisterInstance(Core.Timers.TimerFactory.Default)
            .As<ITimerFactory>();

        RegisterConfigs(builder);
    }

    private void RegisterConfigs(ContainerBuilder builder)
    {
        // TODO: Can't this be done automatically?
        builder.Register<IConfigProvider, IBlocksConfig>((cp) => cp.GetConfig<IBlocksConfig>()).SingleInstance();
        builder.Register<IConfigProvider, IInitConfig>((cp) => cp.GetConfig<IInitConfig>()).SingleInstance();
        builder.Register<IConfigProvider, INetworkConfig>((cp) => cp.GetConfig<INetworkConfig>()).SingleInstance();
    }

    private void SetLoggerVariables(ChainSpec chainSpec, ILogManager logManager)
    {
        logManager.SetGlobalVariable("chain", chainSpec.Name);
        logManager.SetGlobalVariable("chainId", chainSpec.ChainId);
        logManager.SetGlobalVariable("engine", chainSpec.SealEngineType);
    }

}

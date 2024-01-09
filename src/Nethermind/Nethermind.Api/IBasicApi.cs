// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Collections.Generic;
using System.IO.Abstractions;
using System.Linq;
using Autofac;
using Nethermind.Abi;
using Nethermind.Api.Extensions;
using Nethermind.Config;
using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Core.Timers;
using Nethermind.Crypto;
using Nethermind.Db;
using Nethermind.KeyStore;
using Nethermind.Logging;
using Nethermind.Serialization.Json;
using Nethermind.Specs.ChainSpecStyle;
using Nethermind.Stats;
using Nethermind.Synchronization;
using Nethermind.Synchronization.ParallelSync;
using Nethermind.Wallet;

namespace Nethermind.Api
{
    public interface IBasicApi
    {
        DisposableStack DisposeStack { get; }

        IAbiEncoder AbiEncoder { get; }
        IDbProvider? DbProvider { get; set; }
        IRocksDbFactory? RocksDbFactory { get; set; }
        IMemDbFactory? MemDbFactory { get; set; }
        IEthereumEcdsa? EthereumEcdsa { get; set; }
        ProtectedPrivateKey? OriginalSignerKey { get; }
        IReadOnlyList<INethermindPlugin> Plugins { get; }
        string SealEngineType { get; }
        ISyncModeSelector SyncModeSelector { get; set; }
        IBetterPeerStrategy? BetterPeerStrategy { get; set; }

        // TODO: Eventually, no part should use this
        ILifetimeScope BaseContainer { get; }

        // But these in the same place so that we can double check that its no longer used.
        // Every time we migrate to dependency injection, always put it here so that we can keep track what can be
        // removed. Eventually, there should not be any of these left.
        IProcessExitSource? ProcessExit => BaseContainer.Resolve<IProcessExitSource>();
        ISpecProvider? SpecProvider => BaseContainer.Resolve<ISpecProvider>();
        ChainSpec ChainSpec => BaseContainer.Resolve<ChainSpec>();
        ICryptoRandom CryptoRandom => BaseContainer.Resolve<ICryptoRandom>();
        IGasLimitCalculator GasLimitCalculator => BaseContainer.Resolve<IGasLimitCalculator>();
        IConfigProvider ConfigProvider => BaseContainer.Resolve<IConfigProvider>();
        ILogManager LogManager => BaseContainer.Resolve<ILogManager>();
        IJsonSerializer EthereumJsonSerializer => BaseContainer.Resolve<IJsonSerializer>();
        ITimerFactory TimerFactory => BaseContainer.Resolve<ITimerFactory>();
        INodeStatsManager NodeStatsManager => BaseContainer.Resolve<INodeStatsManager>();
        IKeyStore KeyStore => BaseContainer.Resolve<IKeyStore>();
        ITimestamper Timestamper => BaseContainer.Resolve<ITimestamper>();
        IWallet Wallet => BaseContainer.Resolve<IWallet>();
        IFileSystem FileSystem => BaseContainer.Resolve<IFileSystem>();

        public IConsensusPlugin? GetConsensusPlugin() =>
            Plugins
                .OfType<IConsensusPlugin>()
                .SingleOrDefault(cp => cp.SealEngineType == SealEngineType);

        public IEnumerable<IConsensusWrapperPlugin> GetConsensusWrapperPlugins() =>
            Plugins.OfType<IConsensusWrapperPlugin>().Where(p => p.Enabled);

        public IEnumerable<ISynchronizationPlugin> GetSynchronizationPlugins() =>
            Plugins.OfType<ISynchronizationPlugin>();
    }
}

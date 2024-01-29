// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using Autofac;
using Autofac.Features.AttributeFilters;
using Nethermind.Api;
using Nethermind.Blockchain;
using Nethermind.Blockchain.Blocks;
using Nethermind.Blockchain.Find;
using Nethermind.Blockchain.Headers;
using Nethermind.Blockchain.Receipts;
using Nethermind.Config;
using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Crypto;
using Nethermind.Db;
using Nethermind.Db.Blooms;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Repositories;
using Nethermind.TxPool;
using Module = Autofac.Module;

namespace Nethermind.Runner.Modules;

public class CoreModule : Module
{
    private string _baseDbPath;
    private bool _isUsingMemdb;
    private bool _storeReceipts;
    private bool _isMining;
    private bool _persistentBlobTxStorages;
    private bool _useCompactReceiptStore;

    public CoreModule(IConfigProvider configProvider)
    {
        IInitConfig initConfig = configProvider.GetConfig<IInitConfig>();
        _baseDbPath = initConfig.BaseDbPath;
        _isUsingMemdb = initConfig.DiagnosticMode == DiagnosticMode.MemDb;
        _storeReceipts = initConfig.StoreReceipts;
        _persistentBlobTxStorages = configProvider.GetConfig<ITxPoolConfig>().BlobsSupport.IsPersistentStorage();
        _useCompactReceiptStore = configProvider.GetConfig<IReceiptConfig>().CompactReceiptStore;
        _isMining = configProvider.GetConfig<IMiningConfig>().Enabled;
    }

    public CoreModule(
        string baseDbPath = "",
        bool isUsingMemdb = true,
        bool storeReceipts = true,
        bool isMining = true,
        bool persistentBlobTxStorages = true,
        bool useCompactReceiptStore = true
    )
    {
        // Used for testing
        _baseDbPath = baseDbPath;
        _isUsingMemdb = isUsingMemdb;
        _storeReceipts = storeReceipts;
        _isMining = isMining;
        _persistentBlobTxStorages = persistentBlobTxStorages;
        _useCompactReceiptStore = useCompactReceiptStore;
    }

    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder.RegisterType<FollowOtherMiners>()
            .As<IGasLimitCalculator>()
            .SingleInstance();

        builder.RegisterType<NethermindApi>()
            .As<INethermindApi>()
            .SingleInstance();

        ConfigureBlobTxStore(builder);
        ConfigureBloom(builder);
        ConfigureReceipts(builder);

        builder.RegisterType<ChainLevelInfoRepository>()
            .WithAttributeFiltering()
            .As<IChainLevelInfoRepository>()
            .SingleInstance();

        builder.RegisterType<BlockStore>()
            .WithDb(DbNames.Blocks)
            .Keyed<IBlockStore>(IBlockStore.Key.Main)
            .SingleInstance();

        builder.RegisterType<BlockStore>()
            .WithDb(DbNames.BadBlocks)
            .WithParameter(new ParameterFromConfig<IInitConfig>("maxSize", cfg => cfg.BadBlocksStored))
            .Keyed<IBlockStore>(IBlockStore.Key.BadBlock)
            .SingleInstance();

        builder.RegisterType<HeaderStore>()
            .WithAttributeFiltering()
            .As<IHeaderStore>()
            .SingleInstance();

        builder.RegisterType<BlockTree>()
            .WithAttributeFiltering()
            .As<IBlockTree>()
            .As<IBlockFinder>()
            .SingleInstance();

        ConfigureSigner(builder);
    }

    private void ConfigureSigner(ContainerBuilder builder)
    {
        builder.RegisterType<EthereumEcdsa>().AsImplementedInterfaces();
        if (_isMining)
        {
            builder.RegisterType<Signer>()
                .WithAttributeFiltering()
                .As<ISignerStore>()
                .As<ISigner>()
                .SingleInstance();
        }
        else
        {
            builder.RegisterInstance(NullSigner.Instance)
                .As<ISignerStore>()
                .As<ISigner>()
                .SingleInstance();
        }
    }

    private void ConfigureBlobTxStore(ContainerBuilder builder)
    {
        if (_persistentBlobTxStorages)
        {
            builder.RegisterType<BlobTxStorage>()
                .WithAttributeFiltering()
                .As<IBlobTxStorage>()
                .SingleInstance();
        }
        else
        {
            builder.RegisterInstance(NullBlobTxStorage.Instance)
                .As<IBlobTxStorage>();
        }
    }

    private void ConfigureReceipts(ContainerBuilder builder)
    {
        if (_storeReceipts)
        {
            builder.RegisterType<PersistentReceiptStorage>()
                .WithAttributeFiltering()
                .WithParameter(
                    TypedParameter.From(
                        new ReceiptArrayStorageDecoder(_useCompactReceiptStore))
                )
                .As<IReceiptStorage>();
        }
        else
        {
            builder.RegisterInstance(NullReceiptStorage.Instance)
                .As<IReceiptStorage>();
        }


        builder.RegisterType<FullInfoReceiptFinder>().As<IReceiptFinder>();
        builder.RegisterType<ReceiptsRecovery>().As<IReceiptsRecovery>()
            .UsingConstructor(typeof(IEthereumEcdsa), typeof(ISpecProvider), typeof(IReceiptConfig));
        builder.RegisterType<LogFinder>().As<ILogFinder>();
    }

    private void ConfigureBloom(ContainerBuilder builder)
    {
        if (_isUsingMemdb)
        {
            builder.RegisterType<InMemoryDictionaryFileStoreFactory>()
                .As<IFileStoreFactory>()
                .SingleInstance();
        }
        else
        {
            builder.Register((_) => new FixedSizeFileStoreFactory(Path.Combine(_baseDbPath, DbNames.Bloom),
                    DbNames.Bloom, Bloom.ByteLength))
                .As<IFileStoreFactory>()
                .SingleInstance();
        }

        builder.RegisterType<BloomStorage>().WithAttributeFiltering();
        builder.Register<IComponentContext, IBloomConfig, IBloomStorage>(BloomStorageFactory)
            .SingleInstance();
    }

    private IBloomStorage BloomStorageFactory(IComponentContext ctx, IBloomConfig bloomConfig)
    {
        return bloomConfig.Index
            ? ctx.Resolve<BloomStorage>()
            : NullBloomStorage.Instance;
    }
}

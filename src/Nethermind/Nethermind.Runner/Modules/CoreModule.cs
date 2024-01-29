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
using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Crypto;
using Nethermind.Db;
using Nethermind.Db.Blooms;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Repositories;
using Nethermind.TxPool;
using Module = Autofac.Module;

namespace Nethermind.Runner.Modules;

public class CoreModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder.RegisterType<FollowOtherMiners>()
            .As<IGasLimitCalculator>()
            .SingleInstance();

        builder.RegisterType<NethermindApi>()
            .As<INethermindApi>()
            .SingleInstance();

        // Needed to declare AttributeFiltering
        builder.RegisterType<BlobTxStorage>()
            .WithAttributeFiltering()
            .SingleInstance();

        builder.Register<IComponentContext, ITxPoolConfig, IBlobTxStorage>(BlobTxStorageConfig)
            .SingleInstance();

        builder.Register<IInitConfig, IFileStoreFactory>(BloomFileStoreFactory)
            .SingleInstance();
        builder.RegisterType<BloomStorage>().WithAttributeFiltering();
        builder.Register<IComponentContext, IBloomConfig, IBloomStorage>(BloomStorageFactory)
            .SingleInstance();

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
            .SingleInstance();

        builder.RegisterType<EthereumEcdsa>().AsImplementedInterfaces();

        builder.RegisterType<Signer>().SingleInstance();
        builder.Register(SignerFactory);
        builder.Register(SignerStoreFactory);

        builder.RegisterType<PersistentReceiptStorage>().WithAttributeFiltering().SingleInstance();
        builder.Register(ReceiptStorageStoreFactory).SingleInstance();
        builder.RegisterType<FullInfoReceiptFinder>().As<IReceiptFinder>();
        builder.RegisterType<ReceiptsRecovery>().As<IReceiptsRecovery>()
            .UsingConstructor(typeof(IEthereumEcdsa), typeof(ISpecProvider), typeof(IReceiptConfig));
        builder.RegisterType<LogFinder>().As<ILogFinder>();
    }

    private IReceiptStorage ReceiptStorageStoreFactory(IComponentContext ctx)
    {
        if (ctx.Resolve<IInitConfig>().StoreReceipts)
        {
            return ctx.Resolve<PersistentReceiptStorage>(
                TypedParameter.From(
                    new ReceiptArrayStorageDecoder(ctx.Resolve<IReceiptConfig>().CompactReceiptStore)));
        }

        return NullReceiptStorage.Instance;
    }

    private ISigner SignerFactory(IComponentContext ctx)
    {
        if (!ctx.Resolve<IMiningConfig>().Enabled)
        {
            return NullSigner.Instance;
        }

        return ctx.Resolve<Signer>();
    }

    private ISignerStore SignerStoreFactory(IComponentContext ctx)
    {
        if (!ctx.Resolve<IMiningConfig>().Enabled)
        {
            return NullSigner.Instance;
        }

        return ctx.Resolve<Signer>();
    }

    private IBloomStorage BloomStorageFactory(IComponentContext ctx, IBloomConfig bloomConfig)
    {
        return bloomConfig.Index
            ? ctx.Resolve<BloomStorage>()
            : NullBloomStorage.Instance;
    }

    private IFileStoreFactory BloomFileStoreFactory(IInitConfig initConfig)
    {
        return initConfig.DiagnosticMode == DiagnosticMode.MemDb
            ? new InMemoryDictionaryFileStoreFactory()
            : new FixedSizeFileStoreFactory(Path.Combine(initConfig.BaseDbPath, DbNames.Bloom), DbNames.Bloom, Bloom.ByteLength);
    }

    private static IBlobTxStorage BlobTxStorageConfig(IComponentContext ctx, ITxPoolConfig txPoolConfig)
    {
        if (txPoolConfig.BlobsSupport.IsPersistentStorage())
        {
            return ctx.Resolve<BlobTxStorage>();
        }

        return NullBlobTxStorage.Instance;
    }

}

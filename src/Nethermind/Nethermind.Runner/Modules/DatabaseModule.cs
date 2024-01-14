// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Autofac.Core;
using Autofac.Features.AttributeFilters;
using Nethermind.Api;
using Nethermind.Blockchain.Synchronization;
using Nethermind.Config;
using Nethermind.Core;
using Nethermind.Db;
using Nethermind.Db.FullPruning;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rpc;
using Nethermind.JsonRpc.Client;
using Nethermind.TxPool;
using Metrics = Nethermind.Db.Metrics;

namespace Nethermind.Runner.Modules;

public class DatabaseModule : Module
{
    private bool _storeReceipts;
    private DiagnosticMode _diagnosticMode;

    public DatabaseModule(IConfigProvider configProvider)
    {
        IInitConfig initConfig = configProvider.GetConfig<IInitConfig>();
        ISyncConfig syncConfig = configProvider.GetConfig<ISyncConfig>();
        _storeReceipts = initConfig.StoreReceipts || syncConfig.DownloadReceiptsInFastSync;
        _diagnosticMode = initConfig.DiagnosticMode;
    }

    public DatabaseModule()
    {
        // Used for testing
        _storeReceipts = true;
        _diagnosticMode = DiagnosticMode.MemDb;
    }

    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        switch (_diagnosticMode)
        {
            case DiagnosticMode.RpcDb:
                builder.RegisterType<RocksDbFactory>()
                    .WithParameter(GetParameter.FromType<IInitConfig>(ParameterKey.DbPath, initConfig => Path.Combine(initConfig.BaseDbPath, "debug")))
                    .SingleInstance();

                builder.Register(RpcDbFactoryFactory)
                    .As<IDbFactory>()
                    .SingleInstance();
                break;
            case DiagnosticMode.ReadOnlyDb:
                builder.RegisterType<RocksDbFactory>()
                    .WithParameter(GetParameter.FromType<IInitConfig>(ParameterKey.DbPath, initConfig => Path.Combine(initConfig.BaseDbPath, "debug")))
                    .As<IDbFactory>()
                    .SingleInstance();
                break;
            case DiagnosticMode.MemDb:
                builder.RegisterImpl<MemDbFactory, IDbFactory>();
                break;
            default:
                builder.RegisterType<RocksDbFactory>()
                    .WithParameter(GetParameter.FromType<IInitConfig>(ParameterKey.DbPath, initConfig => initConfig.BaseDbPath))
                    .As<IDbFactory>()
                    .SingleInstance();
                break;
        }


        // TODO: Move these to their respective module. They don't need to be in the same place anymore.
        RegisterDb(builder, DbNames.Blocks, () => Metrics.BlocksDbReads++, () => Metrics.BlocksDbWrites++);
        RegisterDb(builder, DbNames.Headers, () => Metrics.HeaderDbReads++, () => Metrics.HeaderDbWrites++);
        RegisterDb(builder, DbNames.BlockNumbers, () => Metrics.BlockNumberDbReads++, () => Metrics.BlockNumberDbWrites++);
        RegisterDb(builder, DbNames.BlockInfos, () => Metrics.BlockInfosDbReads++, () => Metrics.BlockInfosDbWrites++);
        RegisterDb(builder, DbNames.BadBlocks, () => Metrics.BadBlocksDbReads++, () => Metrics.BadBlocksDbWrites++);
        RegisterDb(builder, DbNames.Code, () => Metrics.CodeDbReads++, () => Metrics.CodeDbWrites++);
        RegisterDb(builder, DbNames.Bloom, () => Metrics.BloomDbReads++, () => Metrics.BloomDbWrites++);
        RegisterDb(builder, DbNames.CHT, () => Metrics.CHTDbReads++, () => Metrics.CHTDbWrites++);
        RegisterDb(builder, DbNames.Witness, () => Metrics.WitnessDbReads++, () => Metrics.WitnessDbWrites++);
        RegisterDb(builder, DbNames.Metadata, () => Metrics.MetadataDbReads++, () => Metrics.MetadataDbWrites++);

        RegisterColumnsDb<ReceiptsColumns>(builder, DbNames.Receipts, () => Metrics.ReceiptsDbReads++, () => Metrics.ReceiptsDbWrites++, readOnly: !_storeReceipts);

        // Note: this is lazy
        RegisterColumnsDb<BlobTxsColumns>(builder, DbNames.BlobTransactions, () => Metrics.BlobTransactionsDbReads++, () => Metrics.BlobTransactionsDbWrites++);

        builder.Register<IDbFactory, IFileSystem, FullPruningDb>(StateDbFactory)
            .Keyed<IDb>(DbNames.State)
            .Keyed<IKeyValueStore>(DbNames.State)
            .Keyed<IKeyValueStoreWithBatching>(DbNames.State)
            .As<IFullPruningDb>()
            .SingleInstance();

        builder.Register<IFileSystem, IDbProvider>(InitDbProvider)
            .SingleInstance();
    }

    private static FullPruningDb StateDbFactory(IDbFactory dbFactory, IFileSystem fileSystem)
    {
        DbSettings stateDbSettings = BuildDbSettings(DbNames.State, () => Metrics.StateDbReads++, () => Metrics.StateDbWrites++);
        return new FullPruningDb(
            stateDbSettings,
            dbFactory is not MemDbFactory
                ? new FullPruningInnerDbFactory(dbFactory, fileSystem, stateDbSettings.DbPath)
                : dbFactory,
            () => Interlocked.Increment(ref Metrics.StateDbInPruningWrites));
    }

    private static void RegisterDb(ContainerBuilder builder, string dbName, Action updateReadsMetrics, Action updateWriteMetrics)
    {
        builder.Register<IDbFactory, IDb>((dbFactory) => dbFactory.CreateDb(BuildDbSettings(dbName, updateReadsMetrics, updateWriteMetrics)))
            .Keyed<IDb>(dbName)
            .Keyed<IKeyValueStoreWithBatching>(dbName)
            .Keyed<IKeyValueStore>(dbName)
            .SingleInstance();
    }

    private static void RegisterColumnsDb<T>(ContainerBuilder builder, string dbName, Action updateReadsMetrics, Action updateWriteMetrics, bool readOnly = false) where T : struct, Enum
    {
        Func<IDbFactory, IColumnsDb<T>> factory;
        if (readOnly)
        {
            factory = (_) => new ReadOnlyColumnsDb<T>(new MemColumnsDb<T>(), false);
        }
        else
        {
            factory = (dbFactory) => dbFactory.CreateColumnsDb<T>(BuildDbSettings(dbName, updateReadsMetrics, updateWriteMetrics));
        }

        builder.Register(factory)
            .Named<IColumnsDb<T>>(dbName)
            .As<IColumnsDb<T>>() // You don't need name for columns as T enum exist... Unless for sme reason you want more than one.
            .SingleInstance();
    }

    private static string GetTitleDbName(string dbName) => char.ToUpper(dbName[0]) + dbName[1..];

    private static DbSettings BuildDbSettings(string dbName, Action updateReadsMetrics, Action updateWriteMetrics, bool deleteOnStart = false)
    {
        return new(GetTitleDbName(dbName), dbName)
        {
            UpdateReadMetrics = updateReadsMetrics,
            UpdateWriteMetrics = updateWriteMetrics,
            DeleteOnStart = deleteOnStart
        };
    }

    private IDbProvider InitDbProvider(
        IComponentContext ctx,
        IFileSystem fileSystem
    )
    {
        DbProvider dbProvider = ctx.Resolve<DbProvider>();

        if (_diagnosticMode != DiagnosticMode.ReadOnlyDb)
        {
            return dbProvider;
        }

        return new ReadOnlyDbProvider(dbProvider, _storeReceipts); // ToDo storeReceipts as createInMemoryWriteStore - bug?
    }

    private RpcDbFactory RpcDbFactoryFactory(IComponentContext ctx)
    {
        var rocksDbFactory = ctx.Resolve<RocksDbFactory>();
        var initConfig = ctx.Resolve<IInitConfig>();
        return ctx.Resolve<RpcDbFactory>(
            TypedParameter.From<IDbFactory>(rocksDbFactory),
            TypedParameter.From<IJsonRpcClient>(
                ctx.Resolve<BasicJsonRpcClient>(
                    TypedParameter.From(new Uri(initConfig.RpcDbUrl))
                )
            )
        );
    }
}

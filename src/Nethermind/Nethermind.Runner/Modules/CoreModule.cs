// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac;
using Autofac.Features.AttributeFilters;
using Nethermind.Api;
using Nethermind.Consensus;
using Nethermind.TxPool;

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

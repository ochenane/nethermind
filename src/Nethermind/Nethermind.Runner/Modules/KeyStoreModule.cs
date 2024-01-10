// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.IO.Abstractions;
using Autofac;
using Nethermind.Api;
using Nethermind.Core;
using Nethermind.Crypto;
using Nethermind.KeyStore;
using Nethermind.KeyStore.Config;
using Nethermind.Logging;
using Nethermind.Wallet;

namespace Nethermind.Runner.Modules;

public class KeyStoreModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);
        builder.RegisterType<FileKeyStore>()
            .As<IKeyStore>()
            .SingleInstance();
        builder.Register<ICryptoRandom, ITimestamper, IKeyStoreConfig, ProtectedPrivateKeyFactory>(CreateProtectedKeyFactory)
            .As<IProtectedPrivateKeyFactory>()
            .SingleInstance();
        builder.Register(CreateWallet)
            .SingleInstance();
        builder.Register(CreateNodeKeyManager)
            .SingleInstance();

        builder.Register<INodeKeyManager, ProtectedPrivateKey>((keyManager) => keyManager.LoadNodeKey())
            .Keyed<ProtectedPrivateKey>(PrivateKeyName.NodeKey)
            .SingleInstance();

        builder.Register<INodeKeyManager, ProtectedPrivateKey>((keyManager) => keyManager.LoadSignerKey())
            .Keyed<ProtectedPrivateKey>(PrivateKeyName.SignerKey)
            .SingleInstance();
    }

    private ProtectedPrivateKeyFactory CreateProtectedKeyFactory(ICryptoRandom cryptoRandom, ITimestamper timeStamper, IKeyStoreConfig keyStoreConfig)
    {
        return new ProtectedPrivateKeyFactory(cryptoRandom, timeStamper, keyStoreConfig.KeyStoreDirectory);
    }

    private IWallet CreateWallet(IComponentContext ctx)
    {
        return ctx.Resolve<IInitConfig>() switch
        {
            var config when config.EnableUnsecuredDevWallet && config.KeepDevWalletInMemory => ctx.Resolve<DevWallet>(),
            var config when config.EnableUnsecuredDevWallet && !config.KeepDevWalletInMemory => ctx.Resolve<DevKeyStoreWallet>(),
            _ => ctx.Resolve<ProtectedKeyStoreWallet>()
        };
    }

    private INodeKeyManager CreateNodeKeyManager(IComponentContext ctx)
    {
        // Doing things manually here because there seems to be some kind of interactivity
        IKeyStoreConfig keyStoreConfig = ctx.Resolve<IKeyStoreConfig>();
        IWallet wallet = ctx.Resolve<IWallet>();
        ICryptoRandom cryptoRandom = ctx.Resolve<ICryptoRandom>();
        IFileSystem fileSystem = ctx.Resolve<IFileSystem>();
        IKeyStore keyStore = ctx.Resolve<IKeyStore>();
        ILogManager logManager = ctx.Resolve<ILogManager>();

        new AccountUnlocker(keyStoreConfig, wallet, logManager, new KeyStorePasswordProvider(keyStoreConfig))
            .UnlockAccounts();

        BasePasswordProvider passwordProvider = new KeyStorePasswordProvider(keyStoreConfig)
            .OrReadFromConsole($"Provide password for validator account {keyStoreConfig.BlockAuthorAccount}");

        return new NodeKeyManager(cryptoRandom, keyStore, keyStoreConfig, logManager, passwordProvider, fileSystem);
    }
}

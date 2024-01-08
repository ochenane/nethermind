// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Threading.Tasks;
using Nethermind.Api;
using Nethermind.Blockchain.Services;
using Nethermind.Config;
using Nethermind.Consensus.Processing;
using Nethermind.Consensus.Producers;
using Nethermind.Consensus.Validators;
using Nethermind.Evm;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Init.Steps;
using Nethermind.Merge.Plugin.InvalidChainTracker;

namespace Nethermind.Optimism;

public class InitializeBlockchainOptimism : InitializeBlockchain
{
    private readonly INethermindApi _api;
    private readonly OptimismNethermindApi _opApi;
    private readonly IBlocksConfig _blocksConfig;

    public InitializeBlockchainOptimism(OptimismNethermindApi api) : base(api)
    {
        _api = api;
        _opApi = api;
        _blocksConfig = api.Config<IBlocksConfig>();
    }

    protected override Task InitBlockchain()
    {
        _opApi.SpecHelper = new(_api.ChainSpec.Optimism);
        _opApi.L1CostHelper = new(_opApi.SpecHelper, _api.ChainSpec.Optimism.L1BlockAddress);

        return base.InitBlockchain();
    }

    protected override ITransactionProcessor CreateTransactionProcessor()
    {
        if (_api.SpecProvider is null) throw new StepDependencyException(nameof(_api.SpecProvider));
        if (_opApi.SpecHelper is null) throw new StepDependencyException(nameof(_opApi.SpecHelper));
        if (_opApi.L1CostHelper is null) throw new StepDependencyException(nameof(_opApi.L1CostHelper));

        VirtualMachine virtualMachine = CreateVirtualMachine();

        return new OptimismTransactionProcessor(
            _api.SpecProvider,
            _api.WorldState,
            virtualMachine,
            _api.LogManager,
            _opApi.L1CostHelper,
            _opApi.SpecHelper
        );
    }

    protected override IHeaderValidator CreateHeaderValidator()
    {
        if (_opApi.InvalidChainTracker is null) throw new StepDependencyException(nameof(_opApi.InvalidChainTracker));

        OptimismHeaderValidator opHeaderValidator = new(
            _api.BlockTree,
            _api.SealValidator,
            _api.SpecProvider,
            _api.LogManager);

        return new InvalidHeaderInterceptor(opHeaderValidator, _opApi.InvalidChainTracker, _api.LogManager);
    }

    protected override IBlockValidator CreateBlockValidator()
    {
        if (_opApi.InvalidChainTracker is null) throw new StepDependencyException(nameof(_opApi.InvalidChainTracker));
        if (_api.TxValidator is null) throw new StepDependencyException(nameof(_api.TxValidator));

        OptimismTxValidator txValidator = new(_api.TxValidator);
        BlockValidator blockValidator = new(
            txValidator,
            _api.HeaderValidator,
            _api.UnclesValidator,
            _api.SpecProvider,
            _api.LogManager);

        return new InvalidBlockInterceptor(blockValidator, _opApi.InvalidChainTracker, _api.LogManager);
    }

    protected override BlockProcessor CreateBlockProcessor()
    {
        if (_api.DbProvider is null) throw new StepDependencyException(nameof(_api.DbProvider));
        if (_api.RewardCalculatorSource is null) throw new StepDependencyException(nameof(_api.RewardCalculatorSource));
        if (_api.TransactionProcessor is null) throw new StepDependencyException(nameof(_api.TransactionProcessor));
        if (_opApi.SpecHelper is null) throw new StepDependencyException(nameof(_opApi.SpecHelper));
        if (_api.SpecProvider is null) throw new StepDependencyException(nameof(_api.SpecProvider));
        if (_api.BlockTree is null) throw new StepDependencyException(nameof(_api.BlockTree));
        if (_api.WorldState is null) throw new StepDependencyException(nameof(_api.WorldState));

        Create2DeployerContractRewriter contractRewriter =
            new(_opApi.SpecHelper, _api.SpecProvider, _api.BlockTree);

        return new OptimismBlockProcessor(
            _api.SpecProvider,
            _api.BlockValidator,
            _api.RewardCalculatorSource.Get(_api.TransactionProcessor!),
            new BlockProcessor.BlockValidationTransactionsExecutor(_api.TransactionProcessor, _api.WorldState),
            _api.WorldState,
            _api.ReceiptStorage,
            _api.WitnessCollector,
            _api.LogManager,
            _opApi.SpecHelper,
            contractRewriter);
    }

    protected override IUnclesValidator CreateUnclesValidator() => Always.Valid;

    protected override IHealthHintService CreateHealthHintService() =>
        new ManualHealthHintService(_blocksConfig.SecondsPerSlot * 6, HealthHintConstants.InfinityHint);

    protected override IBlockProductionPolicy CreateBlockProductionPolicy() => AlwaysStartBlockProductionPolicy.Instance;
}

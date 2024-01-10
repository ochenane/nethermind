// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using FluentAssertions;
using FluentAssertions.Execution;
using Nethermind.Api;
using Nethermind.Consensus.AuRa.InitializationSteps;
using Nethermind.Init.Steps;
using Nethermind.Logging;
using NUnit.Framework;

namespace Nethermind.Runner.Test.Ethereum.Steps
{
    [TestFixture, Parallelizable(ParallelScope.All)]
    public class EthereumStepsManagerTests
    {
        [Test]
        public async Task When_no_steps_defined()
        {
            IContainer runnerContext = new ContainerBuilder().Build();

            IEthereumStepsLoader stepsLoader = new EthereumStepsLoader(runnerContext);
            EthereumStepsManager stepsManager = new EthereumStepsManager(
                stepsLoader,
                runnerContext,
                LimboLogs.Instance);

            using CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await stepsManager.InitializeAll(source.Token);
        }

        [Test]
        public async Task With_steps_from_here()
        {
            IContainer runnerContext = CreateNethermindApi();

            IEthereumStepsLoader stepsLoader = new EthereumStepsLoader(runnerContext);
            EthereumStepsManager stepsManager = new EthereumStepsManager(
                stepsLoader,
                runnerContext,
                LimboLogs.Instance);

            using CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(1));

            try
            {
                await stepsManager.InitializeAll(source.Token);
            }
            catch (Exception e)
            {
                if (!(e is OperationCanceledException))
                {
                    throw new AssertionFailedException($"Exception should be {nameof(OperationCanceledException)}");
                }
            }
        }

        [Test]
        [Retry(3)]
        public async Task With_steps_from_here_AuRa()
        {
            IContainer runnerContext = CreateAuraApi();

            IEthereumStepsLoader stepsLoader = new EthereumStepsLoader(runnerContext);
            EthereumStepsManager stepsManager = new EthereumStepsManager(
                stepsLoader,
                runnerContext,
                LimboLogs.Instance);

            using CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

            try
            {
                await stepsManager.InitializeAll(source.Token);
            }
            catch (Exception e)
            {
                e.Should().BeOfType<TestException>();
            }
        }

        [Test]
        public async Task With_failing_steps()
        {
            IContainer runnerContext = CreateNethermindApi();

            IEthereumStepsLoader stepsLoader = new EthereumStepsLoader(runnerContext);
            EthereumStepsManager stepsManager = new EthereumStepsManager(
                stepsLoader,
                runnerContext,
                LimboLogs.Instance);

            using CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            try
            {
                await stepsManager.InitializeAll(source.Token);
            }
            catch (Exception e)
            {
                if (!(e is OperationCanceledException))
                {
                    throw new AssertionFailedException($"Exception should be {nameof(OperationCanceledException)}");
                }
            }
        }

        private static IContainer CreateNethermindApi()
        {
            ContainerBuilder builder = new ContainerBuilder();
            builder.RegisterType<StepLong>().As<IStep>().AsSelf();
            builder.RegisterType<StepForever>().As<IStep>().AsSelf();
            builder.RegisterType<StepA>().As<IStep>().AsSelf();
            builder.RegisterType<StepB>().As<IStep>().AsSelf();
            builder.RegisterType<StepCStandard>().As<IStep>().AsSelf();
            return builder.Build();
        }
        private static IContainer CreateAuraApi()
        {
            ContainerBuilder builder = new ContainerBuilder();
            builder.RegisterType<StepLong>().As<IStep>().AsSelf();
            builder.RegisterType<StepForever>().As<IStep>().AsSelf();
            builder.RegisterType<StepA>().As<IStep>().AsSelf();
            builder.RegisterType<StepB>().As<IStep>().AsSelf();
            builder.RegisterType<StepCAuRa>().As<IStep>().AsSelf();
            return builder.Build();
        }
    }

    public class StepLong : IStep
    {
        public async Task Execute(CancellationToken cancellationToken)
        {
            await Task.Delay(100000, cancellationToken);
        }

        public StepLong(NethermindApi runnerContext)
        {
        }
    }

    public class StepForever : IStep
    {
        public async Task Execute(CancellationToken cancellationToken)
        {
            await Task.Delay(100000);
        }

        public StepForever(NethermindApi runnerContext)
        {
        }
    }

    public class StepA : IStep
    {
        public Task Execute(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public StepA(NethermindApi runnerContext)
        {
        }
    }

    [RunnerStepDependencies(typeof(StepC))]
    public class StepB : IStep
    {
        public Task Execute(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public StepB(NethermindApi runnerContext)
        {
        }
    }

    public abstract class StepC : IStep
    {
        public virtual Task Execute(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public abstract class StepD : IStep
    {
        public virtual Task Execute(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Designed to fail
    /// </summary>
    public class StepCAuRa : StepC
    {
        public StepCAuRa()
        {
        }

        public override async Task Execute(CancellationToken cancellationToken)
        {
            await Task.Run(() => throw new TestException());
        }
    }

    public class StepCStandard : StepC
    {
        public StepCStandard()
        {
        }
    }

    class TestException : Exception
    {
    }
}

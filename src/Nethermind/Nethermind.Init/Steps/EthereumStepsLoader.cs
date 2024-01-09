// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.Linq;
using Autofac;
using Autofac.Core;
using Autofac.Core.Activators.Reflection;

namespace Nethermind.Init.Steps
{
    public class EthereumStepsLoader : IEthereumStepsLoader
    {
        private readonly IEnumerable<Type> _steps;

        public EthereumStepsLoader(ILifetimeScope scope)
        {
            _steps = scope.ComponentRegistry.RegistrationsFor(new TypedService(typeof(IStep)))
                .Where(r => r.Activator is ReflectionActivator)
                .Select(r => ((ReflectionActivator)r.Activator).LimitType)
                .ToList();
        }

        public EthereumStepsLoader(params Type[] steps)
        {
            _steps = steps;
        }

        public IEnumerable<StepInfo> LoadSteps()
        {
            return _steps
                .Select(s => new StepInfo(s, GetStepBaseType(s)))
                .GroupBy(s => s.StepBaseType)
                .Select(g => SelectImplementation(g.ToArray(), g.Key))
                .Where(s => s is not null)
                .Select(s => s!);
        }

        private StepInfo? SelectImplementation(StepInfo[] stepsWithTheSameBase, Type baseType)
        {
            // In case of multiple step declaration, make sure that there is one final step that is the most specific
            // implementation.
            StepInfo[] stepWithNoParent = stepsWithTheSameBase.Where((currentStep) =>
            {
                return !stepsWithTheSameBase.Any(otherStep =>
                    otherStep != currentStep && currentStep.StepType.IsAssignableFrom(otherStep.StepType));
            }).ToArray();

            if (stepWithNoParent.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Unable to find unique step for group {baseType.FullName}. Current steps: {stepWithNoParent}");
            }

            return stepWithNoParent[0];
        }

        private static bool IsStepType(Type t) => typeof(IStep).IsAssignableFrom(t);

        private static Type GetStepBaseType(Type type)
        {
            while (type.BaseType is not null && IsStepType(type.BaseType))
            {
                type = type.BaseType;
            }

            return type;
        }
    }
}

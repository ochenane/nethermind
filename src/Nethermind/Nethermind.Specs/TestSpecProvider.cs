//  Copyright (c) 2021 Demerzel Solutions Limited
//  This file is part of the Nethermind library.
//
//  The Nethermind library is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  The Nethermind library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.

using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Int256;

namespace Nethermind.Specs
{
    public class TestSpecProvider : ISpecProvider
    {
        private ForkActivation? _theMergeBlock = null;

        public TestSpecProvider(IReleaseSpec initialSpecToReturn)
        {
            SpecToReturn = initialSpecToReturn;
            GenesisSpec = initialSpecToReturn;
        }

        public TestSpecProvider(IReleaseSpec initialSpecToReturn, IReleaseSpec finalSpecToReturn)
        {
            SpecToReturn = finalSpecToReturn;
            GenesisSpec = initialSpecToReturn;
        }

        public void UpdateMergeTransitionInfo(long? blockNumber, UInt256? terminalTotalDifficulty = null)
        {
            if (blockNumber is not null)
                _theMergeBlock = blockNumber;
            if (terminalTotalDifficulty is not null)
                TerminalTotalDifficulty = terminalTotalDifficulty;
        }

        public ForkActivation? MergeBlockNumber => _theMergeBlock;
        public UInt256? TerminalTotalDifficulty { get; set; }

        public IReleaseSpec GenesisSpec { get; set; }

        public IReleaseSpec GetSpec(ForkActivation forkActivation) => SpecToReturn;

        public IReleaseSpec SpecToReturn { get; set; }

        public long? DaoBlockNumber { get; set; }
        public ulong ChainId { get; set; }
        public ForkActivation[] TransitionBlocks { get; set; } = new ForkActivation[] { 0 };
        public bool AllowTestChainOverride { get; set; } = true;

        private TestSpecProvider() { }

        public static readonly TestSpecProvider Instance = new();
    }
}

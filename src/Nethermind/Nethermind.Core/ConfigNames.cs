// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

namespace Nethermind.Core;

/// <summary>
/// Utility class to help configuring dependencies. They name here is expected to be registered in some way so that
/// it is easy for component to use it through constructor. Its usually from configs, but sometime it can be from
/// other components or combination of them.
/// </summary>
public enum ConfigNames
{
    // Used by pruning trigger to determine which directory to watch
    FullPruningDbPath,
    FullPruningThresholdMb
}

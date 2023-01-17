// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Db;

namespace Nethermind.Verkle.VerkleDb;

public interface IVerkleKeyValueDb
{
    public IDb LeafDb { get; }
    public IDb StemDb { get; }
    public IDb BranchDb { get; }
}

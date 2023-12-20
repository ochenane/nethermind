// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using FluentAssertions;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Db;
using NUnit.Framework;

namespace Nethermind.Trie.Test;

public class NodeStorageFactoryTests
{
    [TestCase(INodeStorage.KeyScheme.Hash)]
    [TestCase(INodeStorage.KeyScheme.HalfPath)]
    public void Should_DetectHashBasedLayout(INodeStorage.KeyScheme preferredKeyScheme)
    {
        IDb memDb = new MemDb();
        for (int i = 0; i < 20; i++)
        {
            Hash256 hash = Keccak.Compute(i.ToBigEndianByteArray());
            memDb[hash.Bytes] = hash.Bytes.ToArray();
        }

        NodeStorageFactory nodeStorageFactory = new NodeStorageFactory(preferredKeyScheme);
        nodeStorageFactory.DetectCurrentKeySchemeFrom(memDb);
        nodeStorageFactory.WrapKeyValueStore(memDb).Scheme.Should().Be(INodeStorage.KeyScheme.Hash);
    }

    [TestCase(INodeStorage.KeyScheme.Hash)]
    [TestCase(INodeStorage.KeyScheme.HalfPath)]
    public void Should_DetectHalfPathBasedLayout(INodeStorage.KeyScheme preferredKeyScheme)
    {
        IDb memDb = new MemDb();
        for (int i = 0; i < 10; i++)
        {
            Hash256 hash = Keccak.Compute(i.ToBigEndianByteArray());
            memDb[NodeStorage.GetHalfPathNodeStoragePath(null, TreePath.Empty, hash)] = hash.Bytes.ToArray();
        }
        for (int i = 0; i < 10; i++)
        {
            Hash256 hash = Keccak.Compute(i.ToBigEndianByteArray());
            memDb[NodeStorage.GetHalfPathNodeStoragePath(hash, TreePath.Empty, hash)] = hash.Bytes.ToArray();
        }

        NodeStorageFactory nodeStorageFactory = new NodeStorageFactory(preferredKeyScheme);
        nodeStorageFactory.DetectCurrentKeySchemeFrom(memDb);
        nodeStorageFactory.WrapKeyValueStore(memDb).Scheme.Should().Be(INodeStorage.KeyScheme.HalfPath);
    }

    [TestCase(INodeStorage.KeyScheme.Hash)]
    [TestCase(INodeStorage.KeyScheme.HalfPath)]
    public void When_NotEnoughKey_Then_UsePreferredKeyScheme(INodeStorage.KeyScheme preferredKeyScheme)
    {
        IDb memDb = new MemDb();
        for (int i = 0; i < 5; i++)
        {
            Hash256 hash = Keccak.Compute(i.ToBigEndianByteArray());
            memDb[hash.Bytes] = hash.Bytes.ToArray();
        }

        NodeStorageFactory nodeStorageFactory = new NodeStorageFactory(preferredKeyScheme);
        nodeStorageFactory.DetectCurrentKeySchemeFrom(memDb);
        nodeStorageFactory.WrapKeyValueStore(memDb).Scheme.Should().Be(preferredKeyScheme);
    }
}

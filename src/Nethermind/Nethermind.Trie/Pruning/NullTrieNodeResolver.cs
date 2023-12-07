// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core;
using Nethermind.Core.Crypto;

namespace Nethermind.Trie.Pruning
{
    public class NullTrieNodeResolver : ITrieNodeResolver
    {
        private NullTrieNodeResolver() { }

        public static readonly NullTrieNodeResolver Instance = new();

        public TrieNode FindCachedOrUnknown(TreePath path, Hash256 hash) => new(NodeType.Unknown, hash);
        public byte[]? LoadRlp(TreePath path, Hash256 hash, ReadFlags flags = ReadFlags.None) => null;
        public ITrieNodeResolver GetStorageTrieNodeResolver(Hash256 storage)
        {
            return this;
        }
    }

    public class NullFullTrieNodeResolver : ITrieStore
    {
        private NullFullTrieNodeResolver() { }

        public static readonly NullFullTrieNodeResolver Instance = new();

        public void CommitNode(long blockNumber, Hash256? address, NodeCommitInfo nodeCommitInfo,
            WriteFlags writeFlags = WriteFlags.None)
        {
            throw new NotImplementedException();
        }

        public void FinishBlockCommit(TrieType trieType, long blockNumber, Hash256? address, TrieNode? root,
            WriteFlags writeFlags = WriteFlags.None)
        {
            throw new NotImplementedException();
        }

        public bool IsPersisted(Hash256? address, TreePath path, in ValueHash256 keccak)
        {
            throw new NotImplementedException();
        }

        public IReadOnlyTrieStore AsReadOnly(IKeyValueStore? keyValueStore)
        {
            throw new NotImplementedException();
        }

#pragma warning disable CS0067 // Event is never used
        public event EventHandler<ReorgBoundaryReached>? ReorgBoundaryReached;
#pragma warning restore CS0067 // Event is never used

        public IKeyValueStore AsKeyValueStore(Hash256? address)
        {
            throw new NotImplementedException();
        }

        public ISmallTrieStore GetTrieStore(Hash256? address)
        {
            throw new NotImplementedException();
        }

        public TrieNode FindCachedOrUnknown(Hash256? address, TreePath path, Hash256 hash) => new(NodeType.Unknown, hash);
        public byte[]? LoadRlp(Hash256? address, TreePath path, Hash256 hash, ReadFlags flags = ReadFlags.None) => null;
        public ITrieNodeResolver GetStorageTrieNodeResolver(Hash256 storage)
        {
            return NullTrieNodeResolver.Instance;
        }

        public void Dispose()
        {
        }
    }
}

// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core;
using Nethermind.Core.Crypto;

namespace Nethermind.Trie.Pruning
{
    /// <summary>
    /// Safe to be reused for the same wrapped store.
    /// </summary>
    public class ReadOnlyTrieStore : IReadOnlyTrieStore
    {
        private readonly TrieStore _trieStore;
        private readonly NodeStorage? _readOnlyStore;

        public ReadOnlyTrieStore(TrieStore trieStore, IKeyValueStore? readOnlyStore)
        {
            _trieStore = trieStore ?? throw new ArgumentNullException(nameof(trieStore));
            _readOnlyStore = readOnlyStore != null ? new NodeStorage(readOnlyStore) : null;
        }

        public TrieNode FindCachedOrUnknown(Hash256? address, TreePath treePath, Hash256 hash) =>
            _trieStore.FindCachedOrUnknown(address, treePath, hash, true);

        public byte[] LoadRlp(Hash256? address, TreePath treePath, Hash256 hash, ReadFlags flags) =>
            _trieStore.LoadRlp(address, treePath, hash, _readOnlyStore, flags);

        public bool IsPersisted(Hash256? address, TreePath path, in ValueHash256 keccak) => _trieStore.IsPersisted(address, path, keccak);

        public IReadOnlyTrieStore AsReadOnly(IKeyValueStore keyValueStore)
        {
            return new ReadOnlyTrieStore(_trieStore, keyValueStore);
        }

        public void CommitNode(long blockNumber, Hash256? address, NodeCommitInfo nodeCommitInfo, WriteFlags flags = WriteFlags.None) { }

        public void FinishBlockCommit(TrieType trieType, long blockNumber, Hash256? address, TrieNode? root, WriteFlags flags = WriteFlags.None) { }

        public event EventHandler<ReorgBoundaryReached> ReorgBoundaryReached
        {
            add { }
            remove { }
        }

        public IKeyValueStore AsKeyValueStore(Hash256? address) => new ReadOnlyValueStore(_trieStore.AsKeyValueStore(address));
        public ISmallTrieStore GetTrieStore(Hash256? address)
        {
            return new ReadOnlyStorageTrieStore(this, address);
        }

        public void Dispose() { }

        private class ReadOnlyValueStore : IKeyValueStore
        {
            private readonly IKeyValueStore _keyValueStore;

            public ReadOnlyValueStore(IKeyValueStore keyValueStore)
            {
                _keyValueStore = keyValueStore;
            }

            public byte[]? Get(ReadOnlySpan<byte> key, ReadFlags flags = ReadFlags.None) => _keyValueStore.Get(key, flags);

            public void Set(ReadOnlySpan<byte> key, byte[]? value, WriteFlags flags = WriteFlags.None) { }
        }

        public class ReadOnlyStorageTrieStore : ISmallTrieStore
        {
            private ReadOnlyTrieStore _trieStoreImplementation;
            private readonly Hash256? _address;

            public ReadOnlyStorageTrieStore(ReadOnlyTrieStore fullTrieStore, Hash256 address)
            {
                _trieStoreImplementation = fullTrieStore;
                _address = address;
            }

            public TrieNode FindCachedOrUnknown(TreePath path, Hash256 hash)
            {
                return _trieStoreImplementation.FindCachedOrUnknown(_address, path, hash);
            }

            public byte[]? LoadRlp(TreePath path, Hash256 hash, ReadFlags flags = ReadFlags.None)
            {
                return _trieStoreImplementation.LoadRlp(_address, path, hash, flags);
            }

            public ITrieNodeResolver GetStorageTrieNodeResolver(Hash256? address)
            {
                return new ReadOnlyStorageTrieStore(_trieStoreImplementation, _address);
            }

            public void Dispose()
            {
            }

            public void CommitNode(long blockNumber, NodeCommitInfo nodeCommitInfo, WriteFlags writeFlags = WriteFlags.None)
            {
            }

            public void FinishBlockCommit(TrieType trieType, long blockNumber, TrieNode? root, WriteFlags writeFlags = WriteFlags.None)
            {
            }

            public bool IsPersisted(TreePath path, in ValueHash256 keccak)
            {
                return _trieStoreImplementation.IsPersisted(_address, path, in keccak);
            }

            public IKeyValueStore AsKeyValueStore()
            {
                return _trieStoreImplementation.AsKeyValueStore(_address);
            }
        }
    }
}

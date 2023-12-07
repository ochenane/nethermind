// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Serialization.Rlp;

namespace Nethermind.Trie.Pruning;

public class NodeStorage: INodeStorage
{
    protected readonly IKeyValueStore _keyValueStore;

    public NodeStorage(IKeyValueStore keyValueStore)
    {
        _keyValueStore = keyValueStore ?? throw new ArgumentNullException(nameof(keyValueStore));
    }

    private static int _cutLength = int.Parse(Environment.GetEnvironmentVariable("CUT_LENGTH") ?? "4");
    private static bool _centerPath = Environment.GetEnvironmentVariable("CENTER_PATH") == "1";

    public static byte[] GetNodeStoragePath(Hash256? address, TreePath path, ValueHash256 keccak)
    {
        byte[] pathBytes = new byte[40];
        Span<byte> pathSpan = pathBytes;
        TreePath centeredPath = path;
        if (_centerPath)
        {
            // Modify the path so that it would be in the center of the range of its children.
            // This improve cache hit rate by about 10%
            if (centeredPath.Length < 16) centeredPath.Append(8);
        }

        if (address != null)
        {
            // TODO: Calculate the average size of storage
            // Note, pathSpan[0] == 0.
            address.Bytes[..4].CopyTo(pathSpan[1..]);
            centeredPath.Path.BytesAsSpan[..3].CopyTo(pathSpan[5..]);
        } else {
            centeredPath.Path.BytesAsSpan[..7].CopyTo(pathSpan[1..]);

            // Separate the top level tree into its own section. This improve cache hit rate by about a few %.
            if (path.Length <= _cutLength)
            {
                pathBytes[0] = 1;
            }
            else
            {
                pathBytes[0] = 2;
            }
        }

        keccak.Bytes.CopyTo(pathBytes.AsSpan()[8..]);

        return pathBytes;
    }

    public byte[]? Get(Hash256? address, TreePath path, ValueHash256 keccak, ReadFlags readFlags = ReadFlags.None)
    {
        return _keyValueStore.Get(GetNodeStoragePath(address, path, keccak), readFlags);
    }

    public bool KeyExists(Hash256? address, TreePath path, Hash256 keccak)
    {
        return _keyValueStore.KeyExists(GetNodeStoragePath(address, path, keccak));
    }

    public INodeWriteBatch StartWriteBatch()
    {
        return new NodeWriteBatch(((IKeyValueStoreWithBatching)_keyValueStore).StartWriteBatch());
    }

    public void Set(Hash256? address, TreePath path, Hash256 keccak, byte[] toArray, WriteFlags writeFlags)
    {
        _keyValueStore.Set(GetNodeStoragePath(address, path, keccak), toArray, writeFlags);
    }

    public byte[]? GetByHash(ReadOnlySpan<byte> key, ReadFlags flags)
    {
        return _keyValueStore.Get(key, flags);
    }

    public void Flush()
    {
        if (_keyValueStore is IDb db)
        {
            db.Flush();
        }
    }
}

public interface INodeStorage
{
    byte[]? Get(Hash256? address, TreePath path, ValueHash256 keccak, ReadFlags readFlags = ReadFlags.None);
    void Set(Hash256? address, TreePath path, Hash256 hash, byte[] toArray, WriteFlags writeFlags = WriteFlags.None);
    bool KeyExists(Hash256? address, TreePath path, Hash256 hash);
    INodeWriteBatch StartWriteBatch();

    byte[]? GetByHash(ReadOnlySpan<byte> key, ReadFlags flags);
    void Flush();
}

public interface INodeWriteBatch: IDisposable
{
    void Remove(Hash256? address, TreePath path, ValueHash256 persistedHash);
    void Set(Hash256? address, TreePath path, Hash256 currentNodeKeccak, byte[] toArray, WriteFlags writeFlags);
}

public class NodeWriteBatch : INodeWriteBatch
{
    private readonly IWriteBatch _writeBatch;

    public NodeWriteBatch(IWriteBatch writeBatch)
    {
        _writeBatch = writeBatch;
    }

    public void Dispose()
    {
        _writeBatch.Dispose();
    }

    public void Remove(Hash256? address, TreePath path, ValueHash256 keccak)
    {
        _writeBatch.Remove(NodeStorage.GetNodeStoragePath(address, path, keccak));
    }

    public void Set(Hash256? address, TreePath path, Hash256 keccak, byte[] toArray, WriteFlags writeFlags)
    {
        _writeBatch.Set(NodeStorage.GetNodeStoragePath(address, path, keccak), toArray, writeFlags);
    }
}

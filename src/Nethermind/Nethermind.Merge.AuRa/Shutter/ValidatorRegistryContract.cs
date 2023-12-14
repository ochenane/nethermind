// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Nethermind.Abi;
using Nethermind.Blockchain.Contracts;
using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Int256;
using Nethermind.TxPool;

[assembly: InternalsVisibleTo("Nethermind.Merge.AuRa.Test")]

namespace Nethermind.Merge.AuRa.Shutter;

public class ValidatorRegistryContract : CallableContract, IValidatorRegistryContract
{
    private readonly ISigner _signer;
    private readonly ITxSender _txSender;
    private readonly ITxSealer _txSealer;
    private ulong _nonce;
    private readonly ulong _validatorIndex;
    private const string update = "update";
    private const string getNumUpdates = "getNumUpdates";
    private const string getUpate = "getUpdate";
    internal const byte validatorRegistryMessageVersion = 0;

    public ValidatorRegistryContract(ITransactionProcessor transactionProcessor, IAbiEncoder abiEncoder, Address contractAddress, ISigner signer, ITxSender txSender, ITxSealer txSealer, BlockHeader blockHeader)
        : base(transactionProcessor, abiEncoder, contractAddress)
    {
        _signer = signer;
        _txSender = txSender;
        _txSealer = txSealer;
        _validatorIndex = 0; // what is this?

        // set nonce based on last nonce observed from this address
        _nonce = 0;
        UInt256 update = GetNumUpdates(blockHeader);
        for (UInt256 i = update - 1; i >= 0; i -= 1)
        {
            Message m = GetUpdateMessage(blockHeader, i);
            if (m.Sender == ContractAddress!)
            {
                _nonce = m.Nonce + 1;
                break;
            }
        }
    }

    public UInt256 GetNumUpdates(BlockHeader blockHeader)
    {
        object[] res = Call(blockHeader, getNumUpdates, Address.Zero, Array.Empty<object>());
        return new UInt256((byte[])res[0], true);
    }

    public (byte[], byte[]) GetUpdate(BlockHeader blockHeader, in UInt256 i)
    {
        object[] res = Call(blockHeader, getUpate, Address.Zero, new[] {i});
        return ((byte[])res[0], (byte[])res[1]);
    }

    private Message GetUpdateMessage(BlockHeader blockHeader, UInt256 i)
    {
        (byte[] encodedMessage, _) = GetUpdate(blockHeader, i);
        // ignore signature for now, maybe should verify?
        return new Message(encodedMessage[..46]);
    }

    private byte[] Sign(byte[] message)
    {
        // todo: this uses secp256k1, we want BLS
        return _signer.Sign(Keccak.Compute(message)).Bytes;
    }

    private async ValueTask<AcceptTxResult?> Update(byte[] message, byte[] signature)
    {
        Transaction transaction = GenerateTransaction<GeneratedTransaction>(update, _signer.Address, new[] {message, signature});
        await _txSealer.Seal(transaction, TxHandlingOptions.AllowReplacingSignature);
        (Hash256 _, AcceptTxResult? res) = await _txSender.SendTransaction(transaction, TxHandlingOptions.PersistentBroadcast);
        return res;
    }

    public async ValueTask<AcceptTxResult?> Deregister(BlockHeader blockHeader)
    {
        byte[] deregistrationMessage = new Message(ContractAddress!, _validatorIndex, _nonce).ComputeDeregistrationMessage();
        AcceptTxResult? res = await Update(deregistrationMessage, Sign(deregistrationMessage));

        if (res == AcceptTxResult.Accepted)
        {
            _nonce++;
        }

        return res;
    }

    public async ValueTask<AcceptTxResult?> Register(BlockHeader blockHeader)
    {
        byte[] registrationMessage = new Message(ContractAddress!, _validatorIndex, _nonce).ComputeRegistrationMessage();
        AcceptTxResult? res = await Update(registrationMessage, Sign(registrationMessage));

        if (res == AcceptTxResult.Accepted)
        {
            _nonce++;
        }

        return res;
    }

    internal class Message
    {
        public readonly byte Version;
        public readonly ulong ChainId;
        public readonly Address Sender;
        public readonly ulong ValidatorIndex;
        public readonly ulong Nonce;
        public readonly bool IsRegistration;

        public Message(Address sender, ulong validatorIndex, ulong nonce)
        {
            Version = validatorRegistryMessageVersion;
            ChainId = BlockchainIds.Gnosis;
            Sender = sender;
            ValidatorIndex = validatorIndex;
            Nonce = nonce;
        }

        public Message(Span<byte> encodedMessage)
        {
            if (encodedMessage.Length != 46)
            {
                throw new Exception("Encoded validator registry contract message was malformed.");
            }

            Version = encodedMessage[0];
            ChainId = BinaryPrimitives.ReadUInt64BigEndian(encodedMessage[1..]);
            Sender = new Address(encodedMessage[9..29].ToArray());
            ValidatorIndex = BinaryPrimitives.ReadUInt64BigEndian(encodedMessage[29..]);
            Nonce = BinaryPrimitives.ReadUInt64BigEndian(encodedMessage[37..]);
            IsRegistration = encodedMessage[45] == 1;
        }

        private void ComputeRegistryMessagePrefix(Span<byte> registryMessagePrefix)
        {
            registryMessagePrefix[0] = Version;
            BinaryPrimitives.WriteUInt64BigEndian(registryMessagePrefix[1..], ChainId);
            Sender.Bytes.CopyTo(registryMessagePrefix[9..]);
            BinaryPrimitives.WriteUInt64BigEndian(registryMessagePrefix[29..], ValidatorIndex);
            BinaryPrimitives.WriteUInt64BigEndian(registryMessagePrefix[37..], Nonce);
        }

        public byte[] ComputeDeregistrationMessage()
        {
            Span<byte> registryMessagePrefix = stackalloc byte[46];
            ComputeRegistryMessagePrefix(registryMessagePrefix);
            return registryMessagePrefix.ToArray();
        }

        public byte[] ComputeRegistrationMessage()
        {
            Span<byte> registryMessagePrefix = stackalloc byte[46];
            ComputeRegistryMessagePrefix(registryMessagePrefix);
            registryMessagePrefix[45] = 1;
            return registryMessagePrefix.ToArray();
        }
    }

}

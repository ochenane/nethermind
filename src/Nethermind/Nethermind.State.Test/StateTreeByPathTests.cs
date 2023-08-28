// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using FluentAssertions;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.State;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;
using NUnit.Framework;

namespace Nethermind.Store.Test
{
    [TestFixture]
    public class StateTreeByPathTests
    {
        private readonly Account _account0 = Build.An.Account.WithBalance(0).TestObject;
        private readonly Account _account1 = Build.An.Account.WithBalance(1).TestObject;
        private readonly Account _account2 = Build.An.Account.WithBalance(2).TestObject;
        private readonly Account _account3 = Build.An.Account.WithBalance(3).TestObject;

        [SetUp]
        public void Setup()
        {
            Trie.Metrics.TreeNodeHashCalculations = 0;
            Trie.Metrics.TreeNodeRlpDecodings = 0;
            Trie.Metrics.TreeNodeRlpEncodings = 0;
        }

        [Test]
        public void No_reads_when_setting_on_empty()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0);
            tree.Set(TestItem.AddressC, _account0);
            tree.Commit(0);
            Assert.That(db.ReadsCount, Is.EqualTo(0), "reads");
        }

        [Test]
        public void Minimal_writes_when_setting_on_empty()
        {
            MemColumnsDb<StateColumns> stateDb = new();
            MemDb db = (MemDb)stateDb.GetColumnDb(StateColumns.State);
            StateTreeByPath tree = new(new TrieStoreByPath(stateDb, LimboLogs.Instance, 0), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0);
            tree.Set(TestItem.AddressC, _account0);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(8), "writes"); // branch, branch, two leaves (one is stored as RLP)
        }

        [Test]
        public void Minimal_writes_when_setting_on_empty_scenario_2()
        {
            MemColumnsDb<StateColumns> stateDb = new();
            MemDb db = (MemDb)stateDb.GetColumnDb(StateColumns.State);
            StateTreeByPath tree = new(new TrieStoreByPath(stateDb, LimboLogs.Instance, 0), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), _account0);
            tree.Commit(0);

            tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000")).Should().BeEquivalentTo(_account0);
            tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0")).Should().BeEquivalentTo(_account0);
            tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1")).Should().BeEquivalentTo(_account0);
            Assert.That(db.WritesCount, Is.EqualTo(8), "writes"); // extension, branch, leaf, extension, branch, 2x same leaf
            Assert.That(Trie.Metrics.TreeNodeHashCalculations, Is.EqualTo(7), "hashes");
            Assert.That(Trie.Metrics.TreeNodeRlpEncodings, Is.EqualTo(7), "encodings");
        }

        [Test]
        public void Minimal_writes_when_setting_on_empty_scenario_3()
        {
            MemColumnsDb<StateColumns> stateDb = new();
            MemDb db = (MemDb)stateDb.GetColumnDb(StateColumns.State);
            StateTreeByPath tree = new(new TrieStoreByPath(stateDb, LimboLogs.Instance, 0), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), null);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(8), "writes"); // extension, branch, 2x leaf (each node is 2 writes) + deletion writes (2)
            Assert.That(Trie.Metrics.TreeNodeHashCalculations, Is.EqualTo(4), "hashes");
            Assert.That(Trie.Metrics.TreeNodeRlpEncodings, Is.EqualTo(4), "encodings");
        }

        [Test]
        public void Minimal_writes_when_setting_on_empty_scenario_4()
        {
            MemColumnsDb<StateColumns> stateDb = new();
            MemDb db = (MemDb)stateDb.GetColumnDb(StateColumns.State);
            StateTreeByPath tree = new(new TrieStoreByPath(stateDb, LimboLogs.Instance, 0), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), null);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), null);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(6), "writes"); // extension, branch, 2x leaf
            Assert.That(Trie.Metrics.TreeNodeHashCalculations, Is.EqualTo(1), "hashes");
            Assert.That(Trie.Metrics.TreeNodeRlpEncodings, Is.EqualTo(1), "encodings");
        }

        [Test]
        public void Minimal_writes_when_setting_on_empty_scenario_5()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), null);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), null);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), null);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(0), "writes"); // extension, branch, 2x leaf
            Assert.That(Trie.Metrics.TreeNodeHashCalculations, Is.EqualTo(0), "hashes");
            Assert.That(Trie.Metrics.TreeNodeRlpEncodings, Is.EqualTo(0), "encodings");
        }

        [Test]
        public void Scenario_traverse_extension_read_full_match()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"), _account1);
            Account account = tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"));
            //Assert.AreEqual(0, db.ReadsCount);
            Assert.That(account.Balance, Is.EqualTo(_account1.Balance));
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
        }

        [Test]
        public void Scenario_traverse_extension_read_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"), _account1);
            Account account = tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeedddddddddddddddddddddddd"));
            Assert.Null(account);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
        }

        [Test]
        public void Scenario_traverse_extension_new_branching()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"), _account1);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeedddddddddddddddddddddddd"), _account2);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x543c960143a2a06b685d6b92f0c37000273e616bc23888521e7edf15ad06da46"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x543c960143a2a06b685d6b92f0c37000273e616bc23888521e7edf15ad06da46"));
        }

        [Test]
        public void Scenario_traverse_extension_delete_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"), _account1);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeddddddddddddddddddddddddd"), null);
            Assert.That(db.ReadsCount, Is.EqualTo(0));
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xf99f1d3234bad8d63d818db36ff63eefc8916263e654db8b800d3bd03f6339a5"));
        }

        [Test]
        public void Scenario_traverse_extension_create_new_extension()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111"), _account1);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeaaaaaaaaaaaaaaaab00000000"), _account2);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeaaaaaaaaaaaaaaaab11111111"), _account3);
            Assert.That(db.ReadsCount, Is.EqualTo(0));
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x0918112fc898173562441709a2c1cbedb80d1aaecaeadf2f3e9492eeaa568c67"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x0918112fc898173562441709a2c1cbedb80d1aaecaeadf2f3e9492eeaa568c67"));
        }

        [Test]
        public void Scenario_traverse_leaf_update_new_value()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account1);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xaa5c248d4b4b8c27a654296a8e0cc51131eb9011d9166fa0fca56a966489e169"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xaa5c248d4b4b8c27a654296a8e0cc51131eb9011d9166fa0fca56a966489e169"));
        }

        [Test]
        public void Scenario_traverse_leaf_update_no_change()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
        }

        [Test]
        public void Scenario_traverse_leaf_read_matching_leaf()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), null);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"));
        }

        [Test]
        public void Scenario_traverse_leaf_delete_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            tree.Set(new Keccak("1111111111111111111111111111111ddddddddddddddddddddddddddddddddd"), null);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
        }

        [Test]
        public void Scenario_traverse_leaf_update_with_extension()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111111111111111111111111111111"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000000000000000000000000000"), _account1);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x215a4bab4cf2d5ebbaa59c82ae94c9707fcf4cc0ca1fe7e18f918e46db428ef9"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x215a4bab4cf2d5ebbaa59c82ae94c9707fcf4cc0ca1fe7e18f918e46db428ef9"));
        }

        [Test]
        public void Scenario_traverse_leaf_delete_matching_leaf()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            Account account = tree.Get(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"));
            Assert.NotNull(account);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
        }

        [Test]
        public void Scenario_traverse_leaf_read_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("1111111111111111111111111111111111111111111111111111111111111111"), _account0);
            Account account = tree.Get(new Keccak("111111111111111111111111111111111111111111111111111111111ddddddd"));
            Assert.Null(account);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x491fbb33aaff22c0a7ff68d5c81ec114dddf89d022ccdee838a0e9d6cd45cab4"));
        }

        [Test]
        public void Scenario_traverse_branch_update_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111"), _account1);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb22222"), _account2);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xc063af0bd3dd88320bc852ff8452049c42fbc06d1a69661567bd427572824cbf"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0xc063af0bd3dd88320bc852ff8452049c42fbc06d1a69661567bd427572824cbf"));
        }

        [Test]
        public void Scenario_traverse_branch_read_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111"), _account1);
            Account account = tree.Get(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb22222"));
            Assert.Null(account);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x94a193704e99c219d9a21428eb37d6d2d71b3d2cea80c77ff0e201c0df70a283"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x94a193704e99c219d9a21428eb37d6d2d71b3d2cea80c77ff0e201c0df70a283"));
        }

        [Test]
        public void Scenario_traverse_branch_delete_missing()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000"), _account0);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb11111"), _account1);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb22222"), null);
            tree.UpdateRootHash();
            Keccak rootHash = tree.RootHash;
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x94a193704e99c219d9a21428eb37d6d2d71b3d2cea80c77ff0e201c0df70a283"));
            tree.Commit(0);
            Assert.That(rootHash.ToString(true), Is.EqualTo("0x94a193704e99c219d9a21428eb37d6d2d71b3d2cea80c77ff0e201c0df70a283"));
        }

        [Test]
        public void Minimal_hashes_when_setting_on_empty()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            //StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0);
            tree.Set(TestItem.AddressC, _account0);
            tree.Commit(0);
            tree.Get(TestItem.AddressA);
            tree.Get(TestItem.AddressB);
            tree.Get(TestItem.AddressC);
            Assert.That(Trie.Metrics.TreeNodeHashCalculations, Is.EqualTo(5), "hashes"); // branch, branch, three leaves
        }

        [Test]
        public void Minimal_encodings_when_setting_on_empty()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0);
            tree.Set(TestItem.AddressC, _account0);
            tree.Commit(0);
            Assert.That(Trie.Metrics.TreeNodeRlpEncodings, Is.EqualTo(5), "encodings"); // branch, branch, three leaves
        }

        [Test]
        public void Zero_decodings_when_setting_on_empty()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0);
            tree.Set(TestItem.AddressC, _account0);
            tree.Commit(0);
            Assert.That(Trie.Metrics.TreeNodeRlpDecodings, Is.EqualTo(0), "decodings");
        }

        // [Test]
        // public void No_writes_on_continues_update()
        // {
        //     MemDb db = new();
        //     StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
        //     tree.Set(TestItem.AddressA, _account0);
        //     tree.Set(TestItem.AddressA, _account1);
        //     tree.Set(TestItem.AddressA, _account2);
        //     tree.Set(TestItem.AddressA, _account3);
        //     tree.Commit(0);
        //     Assert.AreEqual(2, db.WritesCount, "writes"); // extension, branch, two leaves
        // }

        [Ignore("This is not critical")]
        [Test]
        public void No_writes_on_reverted_update()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(1), "writes before"); // extension, branch, two leaves
            tree.Set(TestItem.AddressA, _account1);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Assert.That(db.WritesCount, Is.EqualTo(1), "writes after"); // extension, branch, two leaves
        }

        [Test]
        public void No_writes_without_commit()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            Assert.That(db.WritesCount, Is.EqualTo(0), "writes");
        }

        [Test]
        public void Can_ask_about_root_hash_without_commiting()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash.ToString(true), Is.EqualTo("0x545a417202afcb10925b2afddb70a698710bb1cf4ab32942c42e9f019d564fdc"));
        }

        [Test]
        public void Can_ask_about_root_hash_without_when_emptied()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), _account0);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.Not.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), _account0);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.Not.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), _account0);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.Not.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb0"), null);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.Not.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb1eeeeeb1"), null);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.Not.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Set(new Keccak("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeb00000000"), null);
            tree.UpdateRootHash();
            Assert.That(tree.RootHash, Is.EqualTo(PatriciaTree.EmptyTreeHash));
            tree.Commit(0);
            Assert.That(tree.RootHash, Is.EqualTo(PatriciaTree.EmptyTreeHash));
        }

        [Test]
        public void hash_empty_tree_root_hash_initially()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            Assert.That(tree.RootHash, Is.EqualTo(PatriciaTree.EmptyTreeHash));
        }

        [Test]
        public void Can_save_null()
        {
            var a = Nibbles.ToEncodedStorageBytes(new byte[] { 5, 3 });
            var b = Nibbles.ToEncodedStorageBytes(new byte[] { 5, 3, 8 });
            var c = Nibbles.ToEncodedStorageBytes(new byte[] { 5, 3, 0 });
            var d = Nibbles.ToEncodedStorageBytes(new byte[] { 5, 3, 0, 12 });
            var e = Nibbles.ToEncodedStorageBytes(new byte[] { 5, 3, 0, 12, 7});

            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, null);
        }

        [Test]
        public void History_update_one_block()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Keccak root0 = tree.RootHash;
            tree.Set(TestItem.AddressA, _account0.WithChangedBalance(20));
            tree.Commit(1);
            Keccak root1 = tree.RootHash;
            Account a0 = tree.Get(TestItem.AddressA, root0);
            Account a1 = tree.Get(TestItem.AddressA, root1);

            Assert.That(_account0.Balance, Is.EqualTo(a0.Balance));
            Assert.That(a1.Balance, Is.EqualTo(new UInt256(20)));
        }

        [Test]
        public void History_update_one_block_before_null()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressB, _account0);
            tree.Commit(0);
            Keccak root0 = tree.RootHash;
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account0.WithChangedBalance(20));
            tree.Commit(1);
            Keccak root1 = tree.RootHash;
            Account a0 = tree.Get(TestItem.AddressA, root0);
            Account a1 = tree.Get(TestItem.AddressA, root1);
            Account b1 = tree.Get(TestItem.AddressB, root1);

            Assert.IsNull(a0);
            Assert.That(a1.Balance, Is.EqualTo(new UInt256(0)));
            Assert.That(b1.Balance, Is.EqualTo(new UInt256(20)));
        }


        [Test]
        public void History_update_non_continous_blocks()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Keccak root0 = tree.RootHash;

            tree.Set(TestItem.AddressB, _account1);
            tree.Commit(1);
            Keccak root1 = tree.RootHash;

            tree.Set(TestItem.AddressA, _account0.WithChangedBalance(20));
            tree.Commit(2);
            Keccak root2 = tree.RootHash;

            Account a0_0 = tree.Get(TestItem.AddressA, root0);
            Account a0_1 = tree.Get(TestItem.AddressA, root1);
            Account a0_2 = tree.Get(TestItem.AddressA, root2);

            Assert.That(_account0.Balance, Is.EqualTo(a0_0.Balance));
            Assert.That(_account0.Balance, Is.EqualTo(a0_1.Balance));

            Assert.That(a0_2.Balance, Is.EqualTo(new UInt256(20)));
        }

        [Test]
        public void History_get_cached_from_root_with_no_changes()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Set(TestItem.AddressB, _account1);
            tree.Set(TestItem.AddressC, _account2);
            tree.Commit(1);
            Keccak root1 = tree.RootHash;

            tree.Set(TestItem.AddressB, _account1.WithChangedBalance(15));
            tree.Commit(2);
            Keccak root2 = tree.RootHash;

            tree.Set(TestItem.AddressC, _account2.WithChangedBalance(20));
            tree.Commit(3);
            Keccak root3 = tree.RootHash;

            Account a0_1 = tree.Get(TestItem.AddressA, root1);
            Account a0_2 = tree.Get(TestItem.AddressA, root2);
            Account a0_3 = tree.Get(TestItem.AddressA, root3);

            Assert.That(a0_1.Balance, Is.EqualTo(_account0.Balance));
            Assert.That(a0_2.Balance, Is.EqualTo(_account0.Balance));
            Assert.That(a0_3.Balance, Is.EqualTo(_account0.Balance));
        }

        [Test]
        public void History_get_on_block_when_account_not_existed()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Keccak root0 = tree.RootHash;

            tree.Set(TestItem.AddressB, _account1);
            tree.Commit(1);
            Account a1_0 = tree.Get(TestItem.AddressB, root0);
            Assert.IsNull(a1_0);

            tree.Set(TestItem.AddressB, _account2);
            tree.Commit(2);

            a1_0 = tree.Get(TestItem.AddressB, root0);

            Assert.IsNull(a1_0);
        }

        [Test]
        public void History_delete_when_max_number_blocks_exceeded()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);
            tree.Set(TestItem.AddressA, _account0);
            tree.Commit(0);
            Keccak root0 = tree.RootHash;
            Keccak root2 = null;

            for (int i = 1; i < 7; i++)
            {
                tree.Set(TestItem.AddressA, _account0.WithChangedBalance((UInt256)i * 5));
                tree.Commit(i);
                if (i == 2)
                    root2 = tree.RootHash;
            }
            Account a1_0 = tree.Get(TestItem.AddressA, root0);
            Account a1_2 = tree.Get(TestItem.AddressA, root2);

            Assert.IsNotNull(a1_0);
            Assert.IsNotNull(a1_2);
            Assert.That(a1_2.Balance, Is.EqualTo((UInt256)(2 * 5)));
        }

        [Test]
        public void CopyStateTest()
        {
            MemColumnsDb<StateColumns> db = new MemColumnsDb<StateColumns>();
            StateTreeByPath tree = new(new TrieStoreByPath(db, LimboLogs.Instance), LimboLogs.Instance);

            tree.Set(TestItem.AddressA, _account1);
            tree.Set(TestItem.AddressB, _account1);
            tree.Set(TestItem.AddressC, _account1);
            tree.Set(TestItem.AddressD, _account1);
            tree.Set(TestItem.AddressA, null);
            tree.Commit(0);
            tree.Get(TestItem.AddressA).Should().BeNull();
            tree.Get(TestItem.AddressB).Balance.Should().BeEquivalentTo(_account1.Balance);
            tree.Get(TestItem.AddressC).Balance.Should().BeEquivalentTo(_account1.Balance);
            tree.Get(TestItem.AddressD).Balance.Should().BeEquivalentTo(_account1.Balance);
        }
    }
}
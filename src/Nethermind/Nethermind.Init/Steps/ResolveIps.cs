// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Nethermind.Api;
using Nethermind.Config;
using Nethermind.Core.Attributes;
using Nethermind.Logging;
using Nethermind.Network;
using Nethermind.Network.Config;
using Nethermind.Network.Discovery;
using Nethermind.Specs.ChainSpecStyle;

namespace Nethermind.Init.Steps
{
    [RunnerStepDependencies]
    public class ResolveIps : IStep
    {
        private readonly IPResolver _ipResolver;
        private readonly IApiWithNetwork _api;
        private readonly ChainSpec _chainSpec;
        private readonly INetworkConfig _networkConfig;
        private readonly IDiscoveryConfig _discoveryConfig;
        private readonly ILogManager _logManager;

        public ResolveIps(
            INethermindApi api,
            IPResolver ipResolver,
            ChainSpec chainSpec,
            INetworkConfig networkConfig,
            IDiscoveryConfig discoveryConfig,
            ILogManager logManager
        )
        {
            _ipResolver = ipResolver;
            _chainSpec = chainSpec;
            _networkConfig = networkConfig;
            _discoveryConfig = discoveryConfig;
            _logManager = logManager;
            _api = api;
        }

        [Todo(Improve.Refactor, "Automatically scan all the references solutions?")]
        public virtual async Task Execute(CancellationToken _)
        {
            // this should be outside of Ethereum Runner I guess
            await _ipResolver.Initialize();
            _networkConfig.ExternalIp = _ipResolver.ExternalIp.ToString();
            _networkConfig.LocalIp = _ipResolver.LocalIp.ToString();

            SetEnode();
            FilterBootNodes();
            UpdateDiscoveryConfig();
        }

        private void SetEnode()
        {
            IPAddress ipAddress = _networkConfig.ExternalIp is not null ? IPAddress.Parse(_networkConfig.ExternalIp) : IPAddress.Loopback;
            IEnode enode = _api.Enode = new Enode(_api.NodeKey!.PublicKey, ipAddress, _networkConfig.P2PPort);

            _logManager.SetGlobalVariable("enode", enode.ToString());
        }

        private void FilterBootNodes()
        {
            if (_api.NodeKey is null)
            {
                return;
            }

            _chainSpec.Bootnodes = _chainSpec.Bootnodes?.Where(n => !n.NodeId?.Equals(_api.NodeKey.PublicKey) ?? false).ToArray() ?? Array.Empty<NetworkNode>();
        }

        private void UpdateDiscoveryConfig()
        {
            if (_discoveryConfig.Bootnodes != string.Empty)
            {
                if (_chainSpec.Bootnodes.Length != 0)
                {
                    _discoveryConfig.Bootnodes += "," + string.Join(",", _chainSpec.Bootnodes.Select(bn => bn.ToString()));
                }
            }
            else
            {
                _discoveryConfig.Bootnodes = string.Join(",", _chainSpec.Bootnodes.Select(bn => bn.ToString()));
            }
        }
    }
}

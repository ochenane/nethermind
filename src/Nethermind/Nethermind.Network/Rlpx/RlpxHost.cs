// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using DotNetty.Buffers;
using DotNetty.Common.Concurrency;
using DotNetty.Handlers.Logging;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Nethermind.Core.Crypto;
using Nethermind.Logging;
using Nethermind.Network.Config;
using Nethermind.Network.P2P;
using Nethermind.Network.P2P.Analyzers;
using Nethermind.Network.P2P.EventArg;
using Nethermind.Network.Rlpx.Handshake;
using Nethermind.Stats.Model;
using Prometheus;
using Exception = System.Exception;
using ILogger = Nethermind.Logging.ILogger;
using LogLevel = DotNetty.Handlers.Logging.LogLevel;

namespace Nethermind.Network.Rlpx
{
    public class RlpxHost : IRlpxHost
    {
        private IChannel? _bootstrapChannel;
        private IEventLoopGroup _bossGroup;
        private IEventLoopGroup _workerGroup;

        private bool _isInitialized;
        public PublicKey LocalNodeId { get; }
        public int LocalPort { get; }
        public string? LocalIp { get; set; }
        private readonly IHandshakeService _handshakeService;
        private readonly IMessageSerializationService _serializationService;
        private readonly ILogManager _logManager;
        private readonly ILogger _logger;
        private readonly ISessionMonitor _sessionMonitor;
        private readonly IDisconnectsAnalyzer _disconnectsAnalyzer;
        private IEventExecutorGroup _group;
        private TimeSpan _sendLatency;
        private readonly TimeSpan _connectTimeout;

        // private PooledByteBufferAllocator _bufferAllocator = new PooledByteBufferAllocator(4, 4, 8192, 8);
        private PooledByteBufferAllocator _bufferAllocator = PooledByteBufferAllocator.Default;

        public RlpxHost(IMessageSerializationService serializationService,
            PublicKey localNodeId,
            int networkProcessingThread,
            int localPort,
            string? localIp,
            int connectTimeoutMs,
            IHandshakeService handshakeService,
            ISessionMonitor sessionMonitor,
            IDisconnectsAnalyzer disconnectsAnalyzer,
            ILogManager logManager,
            TimeSpan sendLatency
        )
        {
            // .NET Core definitely got the easy logging setup right :D
            // ResourceLeakDetector.Level = ResourceLeakDetector.DetectionLevel.Paranoid;
            // ConfigureNamedOptions<ConsoleLoggerOptions> configureNamedOptions = new("", null);
            // OptionsFactory<ConsoleLoggerOptions> optionsFactory = new(
            //     new []{ configureNamedOptions },
            //     Enumerable.Empty<IPostConfigureOptions<ConsoleLoggerOptions>>());
            // OptionsMonitor<ConsoleLoggerOptions> optionsMonitor = new(
            //     optionsFactory,
            //     Enumerable.Empty<IOptionsChangeTokenSource<ConsoleLoggerOptions>>(),
            //     new OptionsCache<ConsoleLoggerOptions>());
            // LoggerFactory loggerFactory = new(
            //     new[] { new ConsoleLoggerProvider(optionsMonitor) },
            //     new LoggerFilterOptions { MinLevel = Microsoft.Extensions.Logging.LogLevel.Warning });
            // InternalLoggerFactory.DefaultFactory = loggerFactory;

            if (networkProcessingThread <= 1)
            {
                _group = new SingleThreadEventLoop();
            }
            else
            {
                _group = new MultithreadEventLoopGroup(networkProcessingThread);
            }
            _serializationService = serializationService ?? throw new ArgumentNullException(nameof(serializationService));
            _logManager = logManager ?? throw new ArgumentNullException(nameof(logManager));
            _logger = logManager.GetClassLogger();
            _sessionMonitor = sessionMonitor ?? throw new ArgumentNullException(nameof(sessionMonitor));
            _disconnectsAnalyzer = disconnectsAnalyzer ?? throw new ArgumentNullException(nameof(disconnectsAnalyzer));
            _handshakeService = handshakeService ?? throw new ArgumentNullException(nameof(handshakeService));
            LocalNodeId = localNodeId ?? throw new ArgumentNullException(nameof(localNodeId));
            LocalPort = localPort;
            LocalIp = localIp;
            _sendLatency = sendLatency;
            _connectTimeout = TimeSpan.FromMilliseconds(connectTimeoutMs);

            PeriodicTimer timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
            Task.Run(async () =>
            {
                while (true)
                {
                    await timer.WaitForNextTickAsync();

                    // RecordMetric(_bufferAllocator, "channel");
                    RecordMetric(PooledByteBufferAllocator.Default, "default");
                }
            });
        }

        private Gauge AllocatorArenaCount =
            Prometheus.Metrics.CreateGauge("allocator_arena_count", "Arena count", "allocator", "alloctype");

        private Gauge AllocatorArenaActiveBytes =
            Prometheus.Metrics.CreateGauge("allocator_arena_active_bytes", "Arena count", "allocator", "alloctype", "arenaidx");
        private Gauge AllocatorArenaActiveAllocation =
            Prometheus.Metrics.CreateGauge("allocator_arena_active_allocation", "Arena count", "allocator", "alloctype", "arenaidx", "type");
        private Gauge AllocatorArenaAllocation =
            Prometheus.Metrics.CreateGauge("allocator_arena_num_allocations", "Arena count", "allocator", "alloctype", "arenaidx");
        private Gauge AllocatorArenaDeallocation =
            Prometheus.Metrics.CreateGauge("allocator_arena_num_deallocations", "Arena count", "allocator", "alloctype", "arenaidx");
        private Gauge AllocatorArenaThreadcache =
            Prometheus.Metrics.CreateGauge("allocator_arena_num_threadcache", "Arena count", "allocator", "alloctype", "arenaidx");
        private Gauge AllocatorArenaChunkList =
            Prometheus.Metrics.CreateGauge("allocator_arena_num_chunk_list", "Arena chunklist", "allocator", "alloctype", "arenaidx");

        private void RecordMetric(PooledByteBufferAllocator allocator, string allocatorName)
        {
            void RecordPoolArenaMetrics(string arenaType, int idx, IPoolArenaMetric poolArena) {
                AllocatorArenaActiveBytes.WithLabels(allocatorName, arenaType, idx.ToString()).Set(poolArena.NumActiveBytes);

                AllocatorArenaActiveAllocation.WithLabels(allocatorName, arenaType, idx.ToString(), "total").Set(poolArena.NumActiveAllocations);
                AllocatorArenaActiveAllocation.WithLabels(allocatorName, arenaType, idx.ToString(), "normal").Set(poolArena.NumActiveNormalAllocations);
                AllocatorArenaActiveAllocation.WithLabels(allocatorName, arenaType, idx.ToString(), "small").Set(poolArena.NumActiveSmallAllocations);
                AllocatorArenaActiveAllocation.WithLabels(allocatorName, arenaType, idx.ToString(), "tiny").Set(poolArena.NumActiveTinyAllocations);
                AllocatorArenaActiveAllocation.WithLabels(allocatorName, arenaType, idx.ToString(), "huge").Set(poolArena.NumActiveHugeAllocations);

                AllocatorArenaAllocation.WithLabels(allocatorName, arenaType, idx.ToString()).Set(poolArena.NumAllocations);
                AllocatorArenaDeallocation.WithLabels(allocatorName, arenaType, idx.ToString()).Set(poolArena.NumDeallocations);
                AllocatorArenaThreadcache.WithLabels(allocatorName, arenaType, idx.ToString()).Set(poolArena.NumThreadCaches);
                AllocatorArenaChunkList.WithLabels(allocatorName, arenaType, idx.ToString()).Set(poolArena.ChunkLists.Sum((it) => it.Count()));
            }

            void RecordMetrics(string arenaType, IReadOnlyCollection<IPoolArenaMetric> poolArena)
            {
                AllocatorArenaCount.WithLabels(allocatorName, arenaType).Set(poolArena.Count);
                int idx = 0;
                foreach (IPoolArenaMetric poolArenaMetric in poolArena)
                {
                    RecordPoolArenaMetrics(arenaType, idx, poolArenaMetric);
                    idx++;
                }
            }

            RecordMetrics("heap", allocator.Metric.HeapArenas());
            RecordMetrics("direct", allocator.Metric.DirectArenas());
        }


        public async Task Init()
        {
            if (_isInitialized)
            {
                throw new InvalidOperationException($"{nameof(PeerManager)} already initialized.");
            }

            _isInitialized = true;

            try
            {
                _bossGroup = new MultithreadEventLoopGroup();
                _workerGroup = new MultithreadEventLoopGroup();

                ServerBootstrap bootstrap = new();
                bootstrap
                    .Group(_bossGroup, _workerGroup)
                    .Channel<TcpServerSocketChannel>()
                    .ChildOption(ChannelOption.SoBacklog, 100)
                    .Option(ChannelOption.Allocator, _bufferAllocator)
                    .ChildOption(ChannelOption.Allocator, _bufferAllocator)
                    .Handler(new LoggingHandler("BOSS", LogLevel.TRACE))
                    .ChildHandler(new ActionChannelInitializer<ISocketChannel>(ch =>
                    {
                        Session session = new(LocalPort, ch, _disconnectsAnalyzer, _logManager);
                        session.RemoteHost = ((IPEndPoint)ch.RemoteAddress).Address.ToString();
                        session.RemotePort = ((IPEndPoint)ch.RemoteAddress).Port;
                        InitializeChannel(ch, session);
                    }));

                Task<IChannel> openTask = LocalIp is null
                    ? bootstrap.BindAsync(LocalPort)
                    : bootstrap.BindAsync(IPAddress.Parse(LocalIp), LocalPort);

                _bootstrapChannel = await openTask.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        AggregateException aggregateException = t.Exception;
                        if (aggregateException?.InnerException is SocketException socketException
                            && socketException.ErrorCode == 10048)
                        {
                            if (_logger.IsError) _logger.Error($"Port {LocalPort} is in use. You can change the port used by adding: --{nameof(NetworkConfig).Replace("Config", string.Empty)}.{nameof(NetworkConfig.P2PPort)} 30303");
                        }
                        else
                        {
                            if (_logger.IsError) _logger.Error($"{nameof(Init)} failed", t.Exception);
                        }

                        return null;
                    }

                    return t.Result;
                });

                if (_bootstrapChannel is null)
                {
                    throw new NetworkingException($"Failed to initialize {nameof(_bootstrapChannel)}", NetworkExceptionType.Other);
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"{nameof(Init)} failed.", ex);
                await Task.WhenAll(_bossGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask, _workerGroup?.ShutdownGracefullyAsync() ?? Task.CompletedTask);
                throw;
            }
        }

        public async Task ConnectAsync(Node node)
        {
            if (_logger.IsTrace) _logger.Trace($"|NetworkTrace| {node:s} initiating OUT connection");

            Bootstrap clientBootstrap = new();
            clientBootstrap.Group(_workerGroup);
            clientBootstrap.Channel<TcpSocketChannel>();
            clientBootstrap.Option(ChannelOption.TcpNodelay, true);
            clientBootstrap.Option(ChannelOption.MessageSizeEstimator, DefaultMessageSizeEstimator.Default);
            clientBootstrap.Option(ChannelOption.Allocator, _bufferAllocator);
            clientBootstrap.Option(ChannelOption.ConnectTimeout, _connectTimeout);
            clientBootstrap.Handler(new ActionChannelInitializer<ISocketChannel>(ch =>
            {
                Session session = new(LocalPort, node, ch, _disconnectsAnalyzer, _logManager);
                InitializeChannel(ch, session);
            }));

            Task<IChannel> connectTask = clientBootstrap.ConnectAsync(node.Address);
            CancellationTokenSource delayCancellation = new();
            Task firstTask = await Task.WhenAny(connectTask, Task.Delay(_connectTimeout.Add(TimeSpan.FromSeconds(2)), delayCancellation.Token));
            if (firstTask != connectTask)
            {
                if (_logger.IsTrace) _logger.Trace($"|NetworkTrace| {node:s} OUT connection timed out");
                throw new NetworkingException($"Failed to connect to {node:s} (timeout)", NetworkExceptionType.Timeout);
            }

            delayCancellation.Cancel();
            if (connectTask.IsFaulted)
            {
                if (_logger.IsTrace)
                {
                    _logger.Trace($"|NetworkTrace| {node:s} error when OUT connecting {connectTask.Exception}");
                }

                throw new NetworkingException($"Failed to connect to {node:s}", NetworkExceptionType.TargetUnreachable, connectTask.Exception);
            }

            if (_logger.IsTrace) _logger.Trace($"|NetworkTrace| {node:s} OUT connected");
        }

        public event EventHandler<SessionEventArgs> SessionCreated;

        private static Counter AbruptDisconnect =
            Prometheus.Metrics.CreateCounter("rlpxhost_abrupt_disconnect", "ABrupt disconnect", "state", "direction");

        private void InitializeChannel(IChannel channel, ISession session)
        {
            if (session.Direction == ConnectionDirection.In)
            {
                Metrics.IncomingConnections++;
            }
            else
            {
                Metrics.OutgoingConnections++;
            }

            if (_logger.IsTrace) _logger.Trace($"|NetworkTrace| Initializing {session} channel");

            _sessionMonitor.AddSession(session);
            session.Disconnected += SessionOnPeerDisconnected;
            SessionCreated?.Invoke(this, new SessionEventArgs(session));

            HandshakeRole role = session.Direction == ConnectionDirection.In ? HandshakeRole.Recipient : HandshakeRole.Initiator;
            NettyHandshakeHandler handshakeHandler = new(_serializationService, _handshakeService, session, role, _logManager, _group, _sendLatency);

            IChannelPipeline pipeline = channel.Pipeline;
            pipeline.AddLast(new LoggingHandler(session.Direction.ToString().ToUpper(), LogLevel.TRACE));
            pipeline.AddLast("enc-handshake-dec", new OneTimeLengthFieldBasedFrameDecoder());
            pipeline.AddLast("enc-handshake-handler", handshakeHandler);

            channel.CloseCompletion.ContinueWith(async x =>
            {
                // The close completion is completed before actual closing or remaining packet is processed.
                // So usually, we do get a disconnect reason from peer, we just receive it after this. So w need to
                // add some delay to account for whatever that is holding the network pipeline.
                await Task.Delay(TimeSpan.FromSeconds(1));

                if (_logger.IsTrace) _logger.Trace($"|NetworkTrace| {session} channel disconnected");

                if (session.State <= SessionState.Initialized)
                {
                    AbruptDisconnect.WithLabels(session.State.ToString(), session.Direction.ToString()).Inc();
                }

                session.MarkDisconnected(DisconnectReason.ConnectionClosed, DisconnectType.Remote, "channel disconnected");
            });
        }

        private void SessionOnPeerDisconnected(object sender, DisconnectEventArgs e)
        {
            ISession session = (Session)sender;
            session.Disconnected -= SessionOnPeerDisconnected;
            session.Dispose();
        }

        public async Task Shutdown()
        {
            //            InternalLoggerFactory.DefaultFactory.AddProvider(new ConsoleLoggerProvider((s, level) => true, false));
            await (_bootstrapChannel?.CloseAsync().ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    _logger.Error($"{nameof(Shutdown)} failed", t.Exception);
                }
            }) ?? Task.CompletedTask);

            if (_logger.IsDebug) _logger.Debug("Closed _bootstrapChannel");

            // every [quietPeriod] we check if there were any event in the loop - if none then we can shutdown
            TimeSpan quietPeriod = TimeSpan.FromMilliseconds(100);
            TimeSpan nettyCloseTimeout = TimeSpan.FromMilliseconds(1000);
            Task closingTask = Task.WhenAll(
                _bossGroup.ShutdownGracefullyAsync(quietPeriod, nettyCloseTimeout),
                _workerGroup.ShutdownGracefullyAsync(nettyCloseTimeout, nettyCloseTimeout));

            // below comment may arise from not understanding the quiet period but the resolution is correct
            // we need to add additional timeout on our side as netty is not executing internal timeout properly, often it just hangs forever on closing
            CancellationTokenSource delayCancellation = new();
            if (await Task.WhenAny(closingTask, Task.Delay(Timeouts.TcpClose, delayCancellation.Token)) != closingTask)
            {
                if (_logger.IsDebug) _logger.Debug($"Could not close rlpx connection in {Timeouts.TcpClose.TotalSeconds} seconds");
            }
            else
            {
                delayCancellation.Cancel();
            }

            if (_logger.IsInfo) _logger.Info("Local peer shutdown complete.. please wait for all components to close");
        }
    }
}

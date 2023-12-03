// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Threading.Tasks;
using DotNetty.Buffers;
using System.Threading;
using System.Threading.Channels;
using DotNetty.Common.Concurrency;
using DotNetty.Transport.Channels;
using Nethermind.Logging;
using Nethermind.Network.P2P.Messages;
using Prometheus;

namespace Nethermind.Network.P2P
{
    public class PacketSender : ChannelHandlerAdapter, IPacketSender
    {
        private readonly IMessageSerializationService _messageSerializationService;
        private readonly ILogger _logger;
        private IChannelHandlerContext _context;
        private TimeSpan _sendLatency;

        private static readonly Histogram MessageSize = Prometheus.Metrics.CreateHistogram(
            "packet_sender_message_size", "handle message latency",
            new HistogramConfiguration()
            {
                LabelNames = new[] { "protocol", "packet_type", "client_type" },
                Buckets = Histogram.PowersOfTenDividedBuckets(1, 8, 10)
            });

        public PacketSender(IMessageSerializationService messageSerializationService, ILogManager logManager,
            TimeSpan sendLatency)
        {
            _messageSerializationService = messageSerializationService ?? throw new ArgumentNullException(nameof(messageSerializationService));
            _logger = logManager.GetClassLogger<PacketSender>() ?? throw new ArgumentNullException(nameof(logManager));
            _sendLatency = sendLatency;
        }

        public int Enqueue<T>(T message) where T : P2PMessage
        {
            if (!_context.Channel.Active)
            {
                return 0;
            }

            IByteBuffer buffer = _messageSerializationService.ZeroSerialize(message);
            MessageSize.WithLabels(message.Protocol, message.PacketType.ToString(), "null").Observe(buffer.ReadableBytes);
            int length = buffer.ReadableBytes;

            // Running in background
            _ = SendBuffer(buffer);

            return length;
        }

        private async Task SendBuffer(IByteBuffer buffer)
        {
            try
            {
                if (_sendLatency != TimeSpan.Zero)
                {
                    // Tried to implement this as a pipeline handler. Got a lot of peering issue for some reason...
                    await Task.Delay(_sendLatency);
                }

                await _context.WriteAndFlushAsync(buffer);
            }
            catch (Exception exception)
            {
                if (_context.Channel is { Active: false })
                {
                    if (_logger.IsTrace) _logger.Trace($"Channel is not active - {exception.Message}");
                }
                else if (_logger.IsError) _logger.Error("Channel is active", exception);
            }
        }

        public override void HandlerAdded(IChannelHandlerContext context)
        {
            _context = context;
        }
    }
}

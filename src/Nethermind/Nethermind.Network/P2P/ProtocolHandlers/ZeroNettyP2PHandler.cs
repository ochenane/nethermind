// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using System.Net.Sockets;
using DotNetty.Buffers;
using DotNetty.Common.Utilities;
using DotNetty.Transport.Channels;
using Nethermind.Core.Exceptions;
using Nethermind.Logging;
using Nethermind.Network.P2P.Subprotocols;
using Nethermind.Network.Rlpx;
using Nethermind.Serialization.Rlp;
using Nethermind.Stats.Model;
using Prometheus;
using Snappy;

namespace Nethermind.Network.P2P.ProtocolHandlers
{
    public class ZeroNettyP2PHandler : SimpleChannelInboundHandler<ZeroPacket>
    {
        private readonly ISession _session;
        private readonly ILogger _logger;

        private Counter ZeroNettyExceptions =
            Prometheus.Metrics.CreateCounter("zero_netty_exceptions", "Zero netty exceptions", "exception", "state", "direction");

        public bool SnappyEnabled { get; private set; }

        public ZeroNettyP2PHandler(ISession session, ILogManager logManager)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _logger = logManager?.GetClassLogger<ZeroNettyP2PHandler>() ?? throw new ArgumentNullException(nameof(logManager));
            lastMeasurement = DateTime.Now;
        }

        public void Init(IPacketSender packetSender, IChannelHandlerContext context)
        {
            // This is the point where P2P is registered.
            // Other capability will be registered on `Hello` message.
            _session.Init(5, context, packetSender);
        }

        public override void ChannelRegistered(IChannelHandlerContext context)
        {
            if (_logger.IsDebug) _logger.Debug($"Registering {nameof(ZeroNettyP2PHandler)}");
            base.ChannelRegistered(context);
        }

        private DateTime lastMeasurement;

        private Histogram InterReadInterval = Prometheus.Metrics.CreateHistogram("zero_netty_inter_read_interavl",
            "read interval", new HistogramConfiguration()
            {
                Buckets = Histogram.PowersOfTenDividedBuckets(0, 8, 10),
            });
        private Histogram LastMeasurementReset = Prometheus.Metrics.CreateHistogram("zero_netty_last_meaturement_reset",
            "reset", new HistogramConfiguration()
            {
                Buckets = Histogram.PowersOfTenDividedBuckets(0, 8, 10),
            });

        protected override void ChannelRead0(IChannelHandlerContext ctx, ZeroPacket input)
        {
            InterReadInterval.Observe((DateTime.Now - lastMeasurement).TotalMicroseconds);
            lastMeasurement = DateTime.Now;

            IByteBuffer content = input.Content;
            if (SnappyEnabled)
            {
                int uncompressedLength = SnappyCodec.GetUncompressedLength(content.Array, content.ArrayOffset + content.ReaderIndex, content.ReadableBytes);
                if (uncompressedLength > SnappyParameters.MaxSnappyLength)
                {
                    throw new Exception("Max message size exceeeded"); // TODO: disconnect here
                }

                if (content.ReadableBytes > SnappyParameters.MaxSnappyLength / 4)
                {
                    if (_logger.IsTrace) _logger.Trace($"Big Snappy message of length {content.ReadableBytes}");
                }
                else
                {
                    if (_logger.IsTrace) _logger.Trace($"Uncompressing with Snappy a message of length {content.ReadableBytes}");
                }


                IByteBuffer output = ctx.Allocator.Buffer(uncompressedLength);

                try
                {
                    int length = SnappyCodec.Uncompress(content.Array, content.ArrayOffset + content.ReaderIndex,
                        content.ReadableBytes, output.Array, output.ArrayOffset + output.WriterIndex);
                    output.SetWriterIndex(output.WriterIndex + length);
                }
                catch (InvalidDataException)
                {
                    output.SafeRelease();
                    // Data is not compressed sometimes, so we pass directly.
                    _session.ReceiveMessage(input);
                    return;
                }
                catch (Exception)
                {
                    content.SkipBytes(content.ReadableBytes);
                    output.SafeRelease();
                    throw;
                }

                content.SkipBytes(content.ReadableBytes);
                ZeroPacket outputPacket = new(output);
                try
                {
                    outputPacket.PacketType = input.PacketType;
                    _session.ReceiveMessage(outputPacket);
                }
                finally
                {
                    outputPacket.SafeRelease();
                }
            }
            else
            {
                _session.ReceiveMessage(input);
            }
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            ZeroNettyExceptions.WithLabels(exception.GetType().Name, _session.State.ToString(), _session.Direction.ToString()).Inc();

            //In case of SocketException we log it as debug to avoid noise
            string clientId = _session?.Node?.ToString(Node.Format.Console) ?? $"unknown {_session?.RemoteHost}";
            if (exception is SocketException)
            {
                if (!exception.ToString().Contains("Connection reset by peer"))
                {
                    if (_logger.IsInfo) _logger.Info($"Error in communication with {clientId} (SocketException): {exception}");
                }
            }
            else
            {
                if (!exception.ToString().Contains("sent an invalid public key format"))
                {
                    if (_logger.IsInfo) _logger.Info($"Error in communication with {clientId}: {exception}");
                }
            }

            if (exception is IInternalNethermindException)
            {
                // Do nothing as we don't want to drop peer for internal issue.
            }
            else if (_session?.Node?.IsStatic != true)
            {
                if (_session?.State == SessionState.Initialized)
                {
                    if (exception is SocketException)
                    {
                        LastMeasurementReset.Observe((DateTime.Now - lastMeasurement).TotalMicroseconds);
                        if (exception.ToString().Contains("Connection reset by peer"))
                        {
                            _session.InitiateDisconnect(DisconnectReason.NettyConnectionReset, $"Error on P2P handler {exception}");
                        }
                        else
                        {
                            _session.InitiateDisconnect(DisconnectReason.NettySocketException, $"Error on P2P handler {exception}");
                        }
                    }
                    else if (exception is RlpException)
                    {
                        _session.InitiateDisconnect(DisconnectReason.NettyRlpException, $"Error on P2P handler {exception}");
                    }
                    else if (exception is SubprotocolException)
                    {
                        _session.InitiateDisconnect(DisconnectReason.NettySubprotocolException, $"Error on P2P handler {exception}");
                    }
                    else if (exception is InvalidDataException)
                    {
                        _session.InitiateDisconnect(DisconnectReason.NettyInvalidDataException, $"Error on P2P handler {exception}");
                    }
                    else
                    {
                        _session.InitiateDisconnect(DisconnectReason.NettyException, $"Error on P2P handler {exception}");
                    }
                }
                else
                {
                    context.DisconnectAsync().ContinueWith(x =>
                    {
                        if (x.IsFaulted && _logger.IsTrace)
                            _logger.Trace($"Error while disconnecting on context on {this} : {x.Exception}");
                    });
                }
            }
            else
            {
                base.ExceptionCaught(context, exception);
            }
        }

        public void EnableSnappy()
        {
            SnappyEnabled = true;
        }
    }
}

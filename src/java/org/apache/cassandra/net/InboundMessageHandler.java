/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.exceptions.IncompatibleSchemaException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.FrameDecoder.Frame;
import org.apache.cassandra.net.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.ResourceLimits.Limit;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * Core logic for handling inbound message deserialization and execution (in tandem with {@link FrameDecoder}).
 *
 * Handles small and large messages, corruption, flow control, dispatch of message processing onto an appropriate
 * thread pool.
 *
 * # Interaction with {@link FrameDecoder}
 *
 * {@link InboundMessageHandler} sits on top of a {@link FrameDecoder} in the Netty pipeline, and is tightly
 * coupled with it.
 *
 * {@link FrameDecoder} decodes inbound frames and relies on a supplied {@link FrameProcessor} to act on them.
 * {@link InboundMessageHandler} provides two implementations of that interface:
 *  - {@link #process(Frame)} is the default, primary processor, and the primary entry point to this class
 *  - {@link UpToOneMessageFrameProcessor}, supplied to the decoder when the handler is reactivated after being
 *    put in waiting mode due to lack of acquirable reserve memory capacity permits
 *
 * Return value of {@link FrameProcessor#process(Frame)} determines whether the decoder should keep processing
 * frames (if {@code true} is returned) or stop until explicitly reactivated (if {@code false} is). To reactivate
 * the decoder (once notified of available resource permits), {@link FrameDecoder#reactivate()} is invoked.
 *
 * # Frames
 *
 * {@link InboundMessageHandler} operates on frames of messages, and there are several kinds of them:
 *  1. {@link IntactFrame} that are contained. As names suggest, these contain one or multiple fully contained
 *     messages believed to be uncorrupted. Guaranteed to not contain an part of an incomplete message.
 *     See {@link #processFrameOfContainedMessages(ShareableBytes, Limit, Limit)}.
 *  2. {@link IntactFrame} that are NOT contained. These are uncorrupted parts of a large message split over multiple
 *     parts due to their size. Can represent first or subsequent frame of a large message.
 *     See {@link #processFirstFrameOfLargeMessage(IntactFrame, Limit, Limit)} and
 *     {@link #processSubsequentFrameOfLargeMessage(Frame)}.
 *  3. {@link CorruptFrame} with corrupt header. These are unrecoverable, and force a connection to be dropped.
 *  4. {@link CorruptFrame} with a valid header, but corrupt payload. These can be either contained or uncontained.
 *     - contained frames with corrupt payload can be gracefully dropped without dropping the connection
 *     - uncontained frames with corrupt payload can be gracefully dropped unless they represent the first
 *       frame of a new large message, as in that case we don't know how many bytes to skip
 *     See {@link #processCorruptFrame(CorruptFrame)}.
 *
 *  Fundamental frame invariants:
 *  1. A contained frame can only have fully-encapsulated messages - 1 to n, that don't cross frame boundaries
 *  2. An uncontained frame can hold a part of one message only. It can NOT, say, contain end of one large message
 *     and a beginning of another one. All the bytes in an uncontained frame always belong to a single message.
 *
 * # Small vs large messages
 *
 * A single handler is equipped to process both small and large messages, potentially interleaved, but the logic
 * differs depending on size. Small messages are deserialized in place, and then handed off to an appropriate
 * thread pool for processing. Large messages accumulate frames until completion of a message, then hand off
 * the untouched frames to the correct thread pool for the verb to be deserialized there and immediately processed.
 *
 * See {@link LargeMessage} for details of the large-message accumulating state-machine, and {@link ProcessMessage}
 * and its inheritors for the differences in execution.
 *
 * # Flow control (backpressure)
 *
 * To prevent nodes from overwhelming and bringing each other to the knees with more inbound messages that
 * can be processed in a timely manner, {@link InboundMessageHandler} implements a strict flow control policy.
 *
 * Before we attempt to process a message fully, we first infer its size from the stream. Then we attempt to
 * acquire memory permits for a message of that size. If we succeed, then we move on actually process the message.
 * If we fail, the frame decoder deactivates until sufficient permits are released for the message to be processed
 * and the handler is activated again. Permits are released back once the message has been fully processed -
 * after the verb handler has been invoked - on the {@link Stage} for the {@link Verb} of the message.
 *
 * Every connection has an exclusive number of permits allocated to it (by default 4MiB). In addition to it,
 * there is a per-endpoint reserve capacity and a global reserve capacity {@link Limit}, shared between all
 * connections from the same host and all connections, respectively. So long as long as the handler stays within
 * its exclusive limit, it doesn't need to tap into reserve capacity.
 *
 * If tapping into reserve capacity is necessary, but the handler fails to acquire capacity from either
 * endpoint of global reserve (and it needs to acquire from both), the handler and its frame decoder become
 * inactive and register with a {@link WaitQueue} of the appropriate type, depending on which of the reserves
 * couldn't be tapped into. Once enough messages have finished processing and had their permits released back
 * to the reserves, {@link WaitQueue} will reactivate the sleeping handlers and they'll resume processing frames.
 *
 * The reason we 'split' reserve capacity into two limits - endpoing and global - is to guarantee liveness, and
 * prevent single endpoint's connections from taking over the whole reserve, starving other connections.
 *
 * One permit per byte of serialized message gets acquired. When inflated on-heap, each message will occupy more
 * than that, necessarily, but despite wide variance, it's a good enough proxy that correlates with on-heap footprint.
 */
public class InboundMessageHandler extends AbstractMessageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private final ConnectionType type;
    private final InetAddressAndPort self;
    private final InetAddressAndPort peer;
    private final int version;

    private final InboundMessageCallbacks callbacks;
    private final Consumer<Message<?>> consumer;

    InboundMessageHandler(FrameDecoder decoder,

                          ConnectionType type,
                          Channel channel,
                          InetAddressAndPort self,
                          InetAddressAndPort peer,
                          int version,
                          int largeThreshold,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnHandlerClosed onClosed,
                          InboundMessageCallbacks callbacks,
                          Consumer<Message<?>> consumer)
    {
        super(decoder,
              channel,
              largeThreshold,
              queueCapacity,
              endpointReserveCapacity,
              globalReserveCapacity,
              endpointWaitQueue,
              globalWaitQueue,
              onClosed);


        this.type = type;
        this.self = self;
        this.peer = peer;
        this.version = version;
        this.callbacks = callbacks;
        this.consumer = consumer;
    }

    protected boolean processOneContainedMessage(ShareableBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = approxTime.now();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        long timeElapsed = currentTimeNanos - header.createdAtNanos;
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        if (approxTime.isAfter(currentTimeNanos, header.expiresAtNanos))
        {
            callbacks.onHeaderArrived(size, header, timeElapsed, NANOSECONDS);
            callbacks.onArrivedExpired(size, header, false, timeElapsed, NANOSECONDS);
            receivedCount++;
            receivedBytes += size;
            bytes.skipBytes(size);
            return true;
        }

        if (!acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
            return false;

        callbacks.onHeaderArrived(size, header, timeElapsed, NANOSECONDS);
        callbacks.onArrived(size, header, timeElapsed, NANOSECONDS);
        receivedCount++;
        receivedBytes += size;

        if (size <= largeThreshold)
            processSmallMessage(bytes, size, header);
        else
            processLargeMessage(bytes, size, header);

        return true;
    }

    private void processSmallMessage(ShareableBytes bytes, int size, Header header)
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + size); // cap to expected message size

        Message<?> message = null;
        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            Message<?> m = serializer.deserialize(in, header, version);
            if (in.available() > 0) // bytes remaining after deser: deserializer is busted
                throw new InvalidSerializedSizeException(header.verb, size, size - in.available());
            message = m;
        }
        catch (IncompatibleSchemaException e)
        {
            callbacks.onFailedDeserialize(size, header, e);
            noSpamLogger.info("{} incompatible schema encountered while deserializing a message", this, e);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            callbacks.onFailedDeserialize(size, header, t);
            logger.error("{} unexpected exception caught while deserializing a message", id(), t);
        }
        finally
        {
            if (null == message)
                releaseCapacity(size);

            // no matter what, set position to the beginning of the next message and restore limit, so that
            // we can always keep on decoding the frame even on failure to deserialize previous message
            buf.position(begin + size);
            buf.limit(end);
        }

        if (null != message)
            dispatch(new ProcessSmallMessage(message, size));
    }

    // for various reasons, it's possible for a large message to be contained in a single frame
    private void processLargeMessage(ShareableBytes bytes, int size, Header header)
    {
        new LargeMessage(size, header, bytes.sliceAndConsume(size).share()).schedule();
    }

    /*
     * Handling of multi-frame large messages
     */

    protected boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ShareableBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = approxTime.now();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        boolean expired = approxTime.isAfter(currentTimeNanos, header.expiresAtNanos);
        if (!expired && !acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
            return false;

        callbacks.onHeaderArrived(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        receivedBytes += buf.remaining();
        largeMessage = new LargeMessage(size, header, expired);
        largeMessage.supply(frame);
        return true;
    }

    protected void processCorruptFrame(CorruptFrame frame) throws Crc.InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (frame.isSelfContained)
        {
            receivedBytes += frame.frameSize;
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
        }
        else if (null == largeMessage) // first frame of a large message
        {
            receivedBytes += frame.frameSize;
            corruptFramesUnrecovered++;
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
            throw new Crc.InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processSubsequentFrameOfLargeMessage(frame);
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
        }
    }

    String id(boolean includeReal)
    {
        if (!includeReal)
            return id();

        return SocketFactory.channelId(peer, (InetSocketAddress) channel.remoteAddress(),
                                       self, (InetSocketAddress) channel.localAddress(),
                                       type, channel.id().asShortText());
    }

    protected String id()
    {
        return SocketFactory.channelId(peer, self, type, channel.id().asShortText());
    }

    @Override
    public String toString()
    {
        return id();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        try
        {
            fatalExceptionCaught(cause);
        }
        catch (Throwable t)
        {
            logger.error("Unexpected exception in {}.exceptionCaught", this.getClass().getSimpleName(), t);
        }
    }

    protected void fatalExceptionCaught(Throwable cause)
    {
        decoder.discard();

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages - closing the connection", id());
        else
            logger.error("{} unexpected exception caught while processing inbound messages; terminating connection", id(), cause);

        channel.close();
    }

    /*
     * A large-message frame-accumulating state machine.
     *
     * Collects intact frames until it's has all the bytes necessary to deserialize the large message,
     * at which point it schedules a task on the appropriate {@link Stage},
     * a task that deserializes the message and immediately invokes the verb handler.
     *
     * Also handles corrupt frames and potential expiry of the large message during accumulation:
     * if it's taking the frames too long to arrive, there is no point in holding on to the
     * accumulated frames, or in gathering more - so we release the ones we already have, and
     * skip any remaining ones, alongside with returning memory permits early.
     */
    private class LargeMessage extends AbstractMessageHandler.LargeMessage<Header>
    {
        private LargeMessage(int size, Header header, boolean isExpired)
        {
            super(size, header, header.expiresAtNanos, isExpired);
        }

        private LargeMessage(int size, Header header, ShareableBytes bytes)
        {
            super(size, header, header.expiresAtNanos, bytes);
        }

        private void schedule()
        {
            dispatch(new ProcessLargeMessage(this));
        }

        protected void onComplete()
        {
            long timeElapsed = approxTime.now() - header.createdAtNanos;

            if (!isExpired && !isCorrupt)
            {
                callbacks.onArrived(size, header, timeElapsed, NANOSECONDS);
                schedule();
            }
            else if (isExpired)
            {
                callbacks.onArrivedExpired(size, header, isCorrupt, timeElapsed, NANOSECONDS);
            }
            else
            {
                callbacks.onArrivedCorrupt(size, header, timeElapsed, NANOSECONDS);
            }
        }

        protected void abort()
        {
            if (!isExpired && !isCorrupt)
                releaseBuffersAndCapacity(); // release resources if in normal state when abort() is invoked
            callbacks.onClosedBeforeArrival(size, header, received, isCorrupt, isExpired);
        }

        private Message deserialize()
        {
            try (ChunkedInputPlus input = ChunkedInputPlus.of(buffers))
            {
                Message<?> m = serializer.deserialize(input, header, version);
                int remainder = input.remainder();
                if (remainder > 0)
                    throw new InvalidSerializedSizeException(header.verb, size, size - remainder);
                return m;
            }
            catch (IncompatibleSchemaException e)
            {
                callbacks.onFailedDeserialize(size, header, e);
                noSpamLogger.info("{} incompatible schema encountered while deserializing a message", InboundMessageHandler.this, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                callbacks.onFailedDeserialize(size, header, t);
                logger.error("{} unexpected exception caught while deserializing a message", id(), t);
            }
            finally
            {
                buffers.clear(); // closing the input will have ensured that the buffers were released no matter what
            }

            return null;
        }
    }

    /**
     * Submit a {@link ProcessMessage} task to the appropriate {@link Stage} for the {@link Verb}.
     */
    private void dispatch(ProcessMessage task)
    {
        Header header = task.header();

        TraceState state = Tracing.instance.initializeFromMessage(header);
        if (state != null) state.trace("{} message received from {}", header.verb, header.from);

        callbacks.onDispatched(task.size(), header);
        header.verb.stage.execute(task, ExecutorLocals.create(state));
    }

    private abstract class ProcessMessage implements Runnable
    {
        /**
         * Actually handle the message. Runs on the appropriate {@link Stage} for the {@link Verb}.
         *
         * Small messages will come pre-deserialized. Large messages will be deserialized on the stage,
         * just in time, and only then processed.
         */
        public void run()
        {
            Header header = header();
            long currentTimeNanos = approxTime.now();
            boolean expired = approxTime.isAfter(currentTimeNanos, header.expiresAtNanos);

            boolean processed = false;
            try
            {
                callbacks.onExecuting(size(), header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);

                if (expired)
                {
                    callbacks.onExpired(size(), header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                    return;
                }

                Message message = provideMessage();
                if (null != message)
                {
                    consumer.accept(message);
                    processed = true;
                    callbacks.onProcessed(size(), header);
                }
            }
            finally
            {
                if (processed)
                    releaseProcessedCapacity(size(), header);
                else
                    releaseCapacity(size());

                releaseResources();

                callbacks.onExecuted(size(), header, approxTime.now() - currentTimeNanos, NANOSECONDS);
            }
        }

        abstract int size();
        abstract Header header();
        abstract Message provideMessage();
        void releaseResources() {}
    }

    private class ProcessSmallMessage extends ProcessMessage
    {
        private final int size;
        private final Message message;

        ProcessSmallMessage(Message message, int size)
        {
            this.size = size;
            this.message = message;
        }

        int size()
        {
            return size;
        }

        Header header()
        {
            return message.header;
        }

        Message provideMessage()
        {
            return message;
        }
    }

    private class ProcessLargeMessage extends ProcessMessage
    {
        private final LargeMessage message;

        ProcessLargeMessage(LargeMessage message)
        {
            this.message = message;
        }

        int size()
        {
            return message.size;
        }

        Header header()
        {
            return message.header;
        }

        Message provideMessage()
        {
            return message.deserialize();
        }

        @Override
        void releaseResources()
        {
            message.releaseBuffers(); // releases buffers if they haven't been yet (by deserialize() call)
        }
    }
}

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

package org.apache.cassandra.transport.compress;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FastByteOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class LZ4FrameCompressorTest
{
    private LZ4FrameCompressor withChecksums = new LZ4FrameCompressor(() -> true);
    private LZ4FrameCompressor withoutChecksums = new LZ4FrameCompressor(() -> false);

    @Test
    public void testRoundTrip() throws Exception
    {
        roundTrip(bytes(), withChecksums, withChecksums);
        roundTrip(bytes(), withoutChecksums, withoutChecksums);
        roundTrip(bytes(), withChecksums, withoutChecksums);
        roundTrip(bytes(), withoutChecksums, withChecksums);
    }

    @Test
    public void testSingleBitCorruptionDetection() throws Exception
    {
        // regardless of the configuration of the decompressor, if checksums
        // are written corruption will be detected
        detectSingleBitCorruption(withChecksums);
        detectSingleBitCorruption(withoutChecksums);
    }

    private void detectSingleBitCorruption(LZ4FrameCompressor decompressor) throws Exception
    {
        for (int i=0; i<100; i++)
        {
            byte[] rawBytes = bytes();
            Frame frame = frame(rawBytes);
            Frame compressed = withChecksums.compress(frame);
            flipSingleBit(compressed);
            try
            {
                decompressor.decompress(compressed).release();
                fail("Expected checksum mismatch during decompression");
            }
            catch (IOException e)
            {
                assertEquals(e.getMessage(), "Block checksum mismatch");
            }
        }
    }

    @Test
    public void testConsecutiveBitCorruptionDetection() throws Exception
    {
        // regardless of the configuration of the decompressor, if checksums
        // are written corruption will be detected
        detectConsecutiveBitCorruption(withChecksums);
        detectConsecutiveBitCorruption(withoutChecksums);
    }

    private void detectConsecutiveBitCorruption(LZ4FrameCompressor decompressor) throws Exception
    {
        // tests flipping all the bits in n consecutive bytes
        for (int i=1; i<100; i++)
        {
            byte[] rawBytes = bytes();
            Frame frame = frame(rawBytes);
            Frame compressed = withChecksums.compress(frame);
            flipConsecutiveBits(compressed, i);
            try
            {
                decompressor.decompress(compressed).release();
                fail("Expected checksum mismatch during decompression");
            }
            catch (IOException e)
            {
                assertEquals(e.getMessage(), "Block checksum mismatch");
            }
        }
    }

    @Test
    public void testSingleBitCorruptionWithoutChecksums() throws Exception
    {
        // regardless of the configuration of the decompressor, if checksums
        // are not written corruption is undetected
        singleBitCorruptionWithoutChecksums(withChecksums);
        singleBitCorruptionWithoutChecksums(withoutChecksums);
    }

    private void singleBitCorruptionWithoutChecksums(LZ4FrameCompressor decompressor) throws Exception
    {
        for (int i=0; i<100; i++)
        {
            byte[] rawBytes = bytes();
            Frame decompressed = null;
            try
            {
                Frame frame = frame(rawBytes);
                Frame compressed = withoutChecksums.compress(frame);
                flipSingleBit(compressed);
                // should decompress without detecting corruption
                decompressed = decompressor.decompress(compressed);
                // frame should not have round tripped successfully though
                assertNotEquals(0, FastByteOperations.compareUnsigned(rawBytes, 0, rawBytes.length,
                                                                      decompressed.body.array(),
                                                                      decompressed.body.arrayOffset(),
                                                                      decompressed.body.readableBytes()));
            }
            finally
            {
                release(decompressed);
            }
        }
    }

    @Test
    public void testConsecutiveBitCorruptionWithoutChecksums() throws Exception
    {
        // regardless of the configuration of the decompressor, if checksums
        // are not written corruption is undetected
        consecutiveBitCorruptionWithoutChecksums(withChecksums);
        consecutiveBitCorruptionWithoutChecksums(withoutChecksums);
    }

    private void consecutiveBitCorruptionWithoutChecksums(LZ4FrameCompressor decompressor) throws Exception
    {
        // tests flipping all the bits in n consecutive bytes
        for (int i=1; i<64; i++)
        {
            byte[] rawBytes = bytes();
            Frame decompressed = null;
            try
            {
                Frame frame = frame(rawBytes);
                Frame compressed = withoutChecksums.compress(frame);

                flipConsecutiveBits(compressed, i);
                // should decompress without detecting corruption
                decompressed = decompressor.decompress(compressed);
                // frame should not have round tripped successfully though
                assertNotEquals(0, FastByteOperations.compareUnsigned(rawBytes, 0, rawBytes.length,
                                                                      decompressed.body.array(),
                                                                      decompressed.body.arrayOffset(),
                                                                      decompressed.body.readableBytes()));
            }
            finally
            {
                release(decompressed);
            }
        }
    }

    private void release(Frame frame)
    {
        if (null != frame)
            frame.release();
    }

    private void flipSingleBit(Frame compressed)
    {
        compressed.body.markWriterIndex();
        compressed.body.markReaderIndex();

        Random r = new Random();
        // grab a random byte from the compressed body
        int index = r.nextInt(compressed.body.readableBytes());
        compressed.body.writerIndex(index);
        byte b = compressed.body.getByte(index);
        // flip a random bit in that byte
        int bit = r.nextInt(8);
        b = (byte)(b ^ (1 << bit));
        // now put it back in the compressed frame body
        compressed.body.writeByte(b);
        compressed.body.resetWriterIndex();
        compressed.body.resetReaderIndex();
    }

    private void flipConsecutiveBits(Frame compressed, int numBytes)
    {
        compressed.body.markWriterIndex();
        compressed.body.markReaderIndex();

        Random r = new Random();
        // grab a random starting point for corruption
        int index = r.nextInt(compressed.body.readableBytes() - numBytes);
        compressed.body.writerIndex(index);
        for (int i=0; i<numBytes; i++)
        {
            byte b = compressed.body.getByte(index + i);
            // flip allthebits
            b = (byte) ~b;
            // re-insert it in place
            compressed.body.writeByte(b);
        }
        compressed.body.resetWriterIndex();
        compressed.body.resetReaderIndex();
    }

    private void roundTrip(byte[] rawBytes, LZ4FrameCompressor compressor, LZ4FrameCompressor decompressor) throws IOException
    {
        Frame decompressed = null;
        Frame original = frame(rawBytes);
        Frame compressed = compressor.compress(original);
        try
        {
            decompressed = decompressor.decompress(compressed);
            assertEquals(0, FastByteOperations.compareUnsigned(rawBytes, 0, rawBytes.length,
                                                               decompressed.body.array(),
                                                               decompressed.body.arrayOffset(),
                                                               decompressed.body.readableBytes()));
        }
        finally
        {
            release(decompressed);
        }
    }

    private Frame frame(byte[] bodyBytes)
    {
        ByteBuf body = CBUtil.allocator.buffer(bodyBytes.length);
        body.writeBytes(bodyBytes);
        return Frame.create(Message.Type.READY, 99, ProtocolVersion.V5, EnumSet.noneOf(Frame.Header.Flag.class), body);
    }

    private byte[] bytes()
    {
        byte[] bytes = new byte[1024 * 1024];
        new Random().nextBytes(bytes);
        return bytes;
    }
}

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
import java.util.function.BooleanSupplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.FLG.Bits;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Frame;

import static net.jpountz.lz4.LZ4FrameOutputStream.FLG.Bits.*;
import static net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;

/**
 * Frame body compressor which encodes message content using the LZ4 framed format.
 *
 * Frames are composed of multiple blocks and this uses the smallest block size
 * supported (64KB). By default, checksums are included for each block individually
 * and for the entire frame content. This can be can be disabled at the node level
 * using the config property native_transport_lz4_checksums_enabled but this is not
 * recommended.
 * Clients wishing to use this compression scheme should specify 'lz4_frame' when connecting.
 * See: https://github.com/lz4/lz4/wiki/lz4_Frame_format.md
 */
public class LZ4FrameCompressor implements FrameCompressor
{
    public static final LZ4FrameCompressor INSTANCE = new LZ4FrameCompressor(DatabaseDescriptor::isNativeTransportLZ4ChecksummingEnabled);

    private static final Bits[] MINIMAL_OPTIONS = new Bits[] {BLOCK_INDEPENDENCE, CONTENT_SIZE};
    private static final Bits[] EXTENDED_OPTIONS = new Bits[] {BLOCK_INDEPENDENCE, CONTENT_SIZE, BLOCK_CHECKSUM, CONTENT_CHECKSUM};

    private final LZ4Compressor compressor;
    private final BooleanSupplier checksumsEnabled;

    LZ4FrameCompressor(BooleanSupplier checksumsEnabled)
    {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        compressor = lz4Factory.fastCompressor();
        this.checksumsEnabled = checksumsEnabled;
    }

    @Override
    public Frame compress(Frame frame) throws IOException
    {
        int uncompressedSize = frame.body.readableBytes();
        int maxCompressedSize = compressor.maxCompressedLength(uncompressedSize);
        ByteBuf compressedBody = CBUtil.allocator.heapBuffer(Integer.BYTES + maxCompressedSize);
        compressedBody.writeByte((byte)(uncompressedSize >>> 24));
        compressedBody.writeByte((byte)(uncompressedSize >>> 16));
        compressedBody.writeByte((byte)(uncompressedSize >>> 8));
        compressedBody.writeByte((byte)uncompressedSize);

        try (ByteBufOutputStream out = new ByteBufOutputStream(compressedBody);
             LZ4FrameOutputStream lz4Stream = new LZ4FrameOutputStream(out,
                                                                       BLOCKSIZE.SIZE_64KB,
                                                                       uncompressedSize,
                                                                       checksumsEnabled.getAsBoolean()
                                                                            ? EXTENDED_OPTIONS
                                                                            : MINIMAL_OPTIONS))
        {
            frame.body.readBytes(lz4Stream, frame.body.readableBytes());
            return frame.with(compressedBody);
        }
        catch (final Throwable e)
        {
            compressedBody.release();
            throw e;
        }
        finally
        {
            //release the old frame
            frame.release();
        }
    }

    @Override
    public Frame decompress(Frame frame) throws IOException
    {
        byte[] b = new byte[4];
        frame.body.readBytes(b);
        int uncompressedLength = ((b[0] & 0xFF) << 24)
                                 | ((b[1] & 0xFF) << 16)
                                 | ((b[2] & 0xFF) <<  8)
                                 | ((b[3] & 0xFF));
        ByteBuf uncompressedBody = CBUtil.allocator.heapBuffer(uncompressedLength);
        int bytesWritten = 0;
        try (LZ4FrameInputStream lz4Stream = new LZ4FrameInputStream(new ByteBufInputStream(frame.body)))
        {
            while (bytesWritten < uncompressedLength)
                bytesWritten += uncompressedBody.writeBytes(lz4Stream, uncompressedBody.writableBytes());

            return frame.with(uncompressedBody);
        }
        catch (final Throwable e)
        {
            uncompressedBody.release();
            throw e;
        }
        finally
        {
            //release the old frame
            frame.release();
        }
    }
}

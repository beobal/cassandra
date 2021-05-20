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
package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;

/**
 * If a client sends a message that can not be parsed by the server then we need to detect this and update metrics
 * for monitoring.
 *
 * An issue was found between 2.1 to 3.0 upgrades with regards to paging serialization. Since
 * this is a serialization issue we hit similar paths by sending bad bytes to the server, so can simulate the mixed-mode
 * paging issue without needing to send proper messages.
 */
@RunWith(Parameterized.class)
public class UnableToParseClientMessageTest extends TestBaseImpl
{
    private static final String ERROR = "Invalid or unsupported protocol version (84)";
    private static Cluster cluster;

    @Parameterized.Parameter(0)
    public ProtocolVersion version;

    @Parameterized.Parameters(name = "{index}: version={0}")
    public static Iterable<ProtocolVersion> params()
    {
        return ProtocolVersion.SUPPORTED;
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.values())).start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    private long getCurrentCount(IInvokableInstance node)
    {
        return  node.callOnInstance(() -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                          .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                          .getCount());
    }

    private SimpleClient client(ProtocolVersion version)
    {
        SimpleClient.Builder builder = SimpleClient.builder("127.0.0.1", 9042);
        if (version.isBeta())
            builder.useBeta();
        return builder.build();
    }

    @Test
    public void badMessageCausesProtocolException() throws IOException, InterruptedException
    {

        IInvokableInstance node = cluster.get(1);
        // make sure everything is fine at the start
        long currentCount = getCurrentCount(node);
        try (SimpleClient client = client(version))
        {
            client.connect(false, true);
            // this should return a failed response
            // in pre-v5 the connection isn't closed, so use `false` to avoid waiting
            Message.Response response = client.execute(new CorruptMessage(), false);
            assertResponse(response);
            node.runOnInstance(() -> {
                // channelRead throws then channelInactive throws after trying to read remaining bytes
                // using spinAssertEquals as the metric is updated AFTER replying back to the client
                // so there is a race where we check the metric before it gets updated
                Util.spinAssertEquals(currentCount + 1L,
                                      () -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                            .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                            .getCount(),
                                      10);
                Assert.assertEquals(0, CassandraMetricsRegistry.Metrics.getMeters()
                                                                       .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                       .getCount());
            });
            List<String> results = node.logs().grep("Protocol exception with client networking").getResult();
            results.forEach(s -> Assert.assertTrue("Expected logs '" + s + "' to contain: Invalid or unsupported protocol version (84)",
                                                   s.contains("Invalid or unsupported protocol version (84)")));
        }
    }

    private void assertResponse(Message.Response response)
    {
        Assert.assertEquals(Message.Type.ERROR, response.type);
        Assert.assertTrue(response.toString().contains(ERROR));
    }

    private static class CorruptMessage extends OptionsMessage
    {
        final ByteBuf encodedForm = Unpooled.wrappedBuffer(new byte[] {84, 104, 105, 115, 32, 105, 115, 32, 106 });

        @Override
        public Envelope encode(ProtocolVersion version)
        {
            Envelope base = super.encode(version);
            return new CorruptEnvelope(base.header, base.body, encodedForm);
        }

        private static class CorruptEnvelope extends Envelope
        {
            final ByteBuf encoded;
            public CorruptEnvelope(Header header, ByteBuf body, ByteBuf encoded)
            {
                super(header, body);
                this.encoded = encoded;
            }

            // for V4 and below
            @Override
            public ByteBuf encodeHeader()
            {
                return encoded;
            }

            // for V5 and above
            @Override
            public void encodeInto(ByteBuffer buf)
            {
                buf.put(encoded.nioBuffer());
            }
        }
    }
}

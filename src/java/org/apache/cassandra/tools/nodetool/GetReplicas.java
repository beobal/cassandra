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

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "getreplicas", description = "Print replicas for a given key")
public class GetReplicas extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table> <key>", description = "The keyspace, the table, and the partition key for which we need to find replicas")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 3, "getendpoints requires keyspace, table and partition key arguments");
        String ks = args.get(0);
        String table = args.get(1);
        String key = args.get(2);

        System.out.println(probe.getReplicas(ks, table, key));
    }
}

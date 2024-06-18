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
package org.apache.cassandra.locator;

/**
 * An endpoint snitch tells Cassandra information about network topology that it can use to route
 * requests more efficiently.
 */
public abstract class AbstractNetworkTopologySnitch extends AbstractEndpointSnitch
{
    private static final NodeProximity proximity = new NetworkTopologyProximity();
    @Override
    public int compareEndpoints(InetAddressAndPort address, Replica r1, Replica r2)
    {
        return proximity.compareEndpoints(address, r1, r2);
    }
}

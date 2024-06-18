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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.membership.Location;

import static java.lang.String.format;

public class CloudMetadataLocationProvider implements InitialLocationProvider
{
    static final Logger logger = LoggerFactory.getLogger(CloudMetadataLocationProvider.class);

    static final String DEFAULT_DC = "UNKNOWN-DC";
    static final String DEFAULT_RACK = "UNKNOWN-RACK";

    protected final AbstractCloudMetadataServiceConnector connector;

    public final Location location;

    public CloudMetadataLocationProvider(AbstractCloudMetadataServiceConnector connector, Location location)
    {
        this.connector = connector;
        this.location = location;
        logger.info(format("%s using datacenter: %s, rack: %s, connector: %s, properties: %s",
                           getClass().getName(), location.datacenter, location.rack, connector, connector.getProperties()));
    }

    @Override
    public final Location initialLocation()
    {
        return location;
    }
}

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

package org.apache.cassandra.schema;

import java.util.EnumSet;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.exceptions.ConfigurationException;

// The types of read commands for which read repairs may be performed
public class ReadRepairableCommandsParam
{
    public enum Kind
    {
        ALL, READ, RANGE, NONE;
    }

    public static final ReadRepairableCommandsParam ALL = all();
    public static final ReadRepairableCommandsParam READ = read();
    public static final ReadRepairableCommandsParam RANGE = range();
    public static final ReadRepairableCommandsParam NONE = none();

    private final Kind kind;
    private final Function<ReadCommand, Boolean> decisionFunction;

    private ReadRepairableCommandsParam(Kind kind, Function<ReadCommand, Boolean> decisionFunction)
    {
        this.kind = kind;
        this.decisionFunction = decisionFunction;
    }

    public boolean mayRepair(ReadCommand command)
    {
        return decisionFunction.apply(command);
    }

    public static ReadRepairableCommandsParam none()
    {
        return new ReadRepairableCommandsParam(Kind.NONE, command -> false);
    }

    public static ReadRepairableCommandsParam all()
    {
        return new ReadRepairableCommandsParam(Kind.ALL, command -> true);
    }

    public static ReadRepairableCommandsParam read()
    {
        return new ReadRepairableCommandsParam(Kind.READ, command -> command instanceof SinglePartitionReadCommand);
    }

    public static ReadRepairableCommandsParam range()
    {
        return new ReadRepairableCommandsParam(Kind.RANGE, command -> command instanceof PartitionRangeReadCommand);
    }

    public String toString()
    {
        return kind.name();
    }

    public static ReadRepairableCommandsParam fromString(String value)
    {
        try
        {
            switch (Kind.valueOf(value))
            {
                case ALL:
                    return ALL;
                case READ:
                    return READ;
                case RANGE:
                    return RANGE;
                case NONE:
                    return NONE;
            }
            throw new AssertionError("Illegal kind for repairable commands parameter: " + value);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value %s for option %s. Valid values are %s",
                                                           value, TableParams.Option.READ_REPAIRABLE_COMMANDS,
                                                           StringUtils.join(Kind.values(), ',')));
        }
    }

    private static final EnumSet<ReadRepairableCommandsParam.Kind> READS = EnumSet.of(Kind.ALL, Kind.READ);
    public boolean includesReadCommands()
    {
       return READS.contains(this.kind);
    }
}

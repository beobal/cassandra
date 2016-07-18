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
package org.apache.cassandra.index.sasi.plan;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;

public class QueryPlan
{
    private static final Logger logger = LoggerFactory.getLogger(QueryPlan.class);
    private final QueryController controller;

    public QueryPlan(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs)
    {
        this.controller = new QueryController(cfs, (PartitionRangeReadCommand) command, executionQuotaMs);
    }

    /**
     * Converts expressions into operation tree (which is currently just a single AND).
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        try
        {
            Operation.Builder and = new Operation.Builder(OperationType.AND, controller);
            controller.getExpressions().forEach(and::add);
            return and.complete();
        }
        catch (Exception | Error e)
        {
            controller.finish();
            throw e;
        }
    }

    public UnfilteredPartitionIterator execute(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return new ResultIterator(analyze(), controller, executionController);
    }

    private static class ResultIterator implements UnfilteredPartitionIterator
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        private final Operation operationTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;

        private Iterator<RowKey> currentKeys = null;
        private UnfilteredRowIterator nextPartition = null;
        private DecoratedKey lastPartitionKey = null;

        public ResultIterator(Operation operationTree, QueryController controller, ReadExecutionController executionController)
        {
            this.keyRange = controller.dataRange().keyRange();

            this.operationTree = operationTree;
            this.controller = controller;
            this.executionController = executionController;
            if (operationTree != null)
                operationTree.skipTo((Long) keyRange.left.getToken().getTokenValue());
        }

        public boolean hasNext()
        {
            return prepareNext();
        }

        public UnfilteredRowIterator next()
        {
            if (nextPartition == null)
                prepareNext();

            UnfilteredRowIterator toReturn = nextPartition;
            nextPartition = null;
            return toReturn;
        }

        private boolean prepareNext()
        {
            if (operationTree == null)
                return false;

            while (true)
            {
                if (currentKeys == null || !currentKeys.hasNext())
                {
                    if (!operationTree.hasNext())
                        return false;

                    Token token = operationTree.next();
                    currentKeys = token.iterator();
                }

                CFMetaData metadata = controller.metadata();
                BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(metadata.comparator);

                while (true)
                {
                    if (!currentKeys.hasNext())
                    {
                        // No more keys for this token.
                        // If no clusterings were collected yet, exit this inner loop so the operation
                        // tree iterator can move on to the next token.
                        // If some clusterings were collected, build an iterator for those rows
                        // and return.
                        if (clusterings.isEmpty() || lastPartitionKey == null)
                            break;

                        UnfilteredRowIterator partition = fetchPartition(lastPartitionKey, clusterings.build());
                        if (partition.isEmpty())
                        {
                            partition.close();
                            continue;
                        }

                        nextPartition = partition;
                        return true;
                    }

                    RowKey fullKey = currentKeys.next();
                    DecoratedKey key = fullKey.decoratedKey;

                    // TODO
                    if (!keyRange.right.isMinimum() && keyRange.right.compareTo(key) < 0)
                        return false;

                    if (lastPartitionKey != null && metadata.getKeyValidator().compare(lastPartitionKey.getKey(), key.getKey()) != 0)
                    {
                        UnfilteredRowIterator partition = fetchPartition(lastPartitionKey, clusterings.build());
                        // reset clusterings
                        clusterings = BTreeSet.builder(metadata.comparator);

                        if (partition.isEmpty())
                        {
                            partition.close();
                        }
                        else
                        {
                            nextPartition = partition;
                            return true;
                        }
                    }

                    lastPartitionKey = key;
                    clusterings.add(fullKey.clustering);
                }
            }
        }

        private UnfilteredRowIterator fetchPartition(DecoratedKey key, NavigableSet<Clustering> clusterings)
        {
            // TODO static rows are broken - i.e. if one of the clusterings in the set is STATIC_CLUSTERING
            // an assert is triggered in ClusteringIndexNamesFilter, so these need special casing
            return Transformation.apply(controller.getPartition(key, clusterings, executionController), new Transform());
        }

        public void close()
        {
            if (nextPartition != null)
                nextPartition.close();
        }

        public boolean isForThrift()
        {
            return controller.isForThrift();
        }

        public CFMetaData metadata()
        {
            return controller.metadata();
        }

        private class Transform extends Transformation
        {
            @Override
            public Row applyToRow(Row row)
            {
                // todo fix static rows - needs change to operationTree.satisfiedBy
                if (operationTree.satisfiedBy(row, Rows.EMPTY_STATIC_ROW, true))
                    return row;

                return null;
            }
        }
    }
}

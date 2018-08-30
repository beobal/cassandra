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

package org.apache.cassandra.db.reads;


import java.util.*;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;

public abstract class TimestampOrderedPartitionReader
{
    protected final ColumnFamilyStore cfs;
    protected final TableMetadata metadata;
    protected final DecoratedKey partitionKey;
    protected final ColumnFilter columnFilter;
    protected ClusteringIndexNamesFilter filter;

    final SSTableReadsListener metricsCollector;
    boolean onlyUnrepaired;
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    TimestampOrderedPartitionReader(ColumnFamilyStore cfs,
                                    DecoratedKey partitionKey,
                                    ClusteringIndexNamesFilter filter,
                                    ColumnFilter columnFilter,
                                    SSTableReadsListener metricsCollector)
    {
        this.cfs = cfs;
        this.metadata = cfs.metadata();
        this.partitionKey = partitionKey;
        this.filter = filter;
        this.columnFilter = columnFilter;
        this.metricsCollector = metricsCollector;
    }

    public static class Builder
    {
        private final ColumnFamilyStore cfs;
        private final DecoratedKey key;
        private final ClusteringIndexNamesFilter filter;
        private final ColumnFilter columnFilter;
        private final SSTableReadsListener metricsCollector;
        private RepairedDataInfo repairedDataInfo;

        public Builder(ColumnFamilyStore cfs,
                       DecoratedKey key,
                       ClusteringIndexNamesFilter filter,
                       ColumnFilter columnFilter,
                       SSTableReadsListener metricsCollector)
        {
            this.cfs = cfs;
            this.key = key;
            this.filter = filter;
            this.columnFilter = columnFilter;
            this.metricsCollector = metricsCollector;
        }

        public Builder withRepairedDataTracking(RepairedDataInfo info)
        {
            this.repairedDataInfo = info;
            return this;
        }

        public TimestampOrderedPartitionReader build()
        {
            return repairedDataInfo == null
                   ? new WithoutRepairedDataTracking(cfs, key, filter, columnFilter, metricsCollector)
                   : new WithRepairedDataTracking(cfs, key, filter, columnFilter, metricsCollector, repairedDataInfo);
        }
    }

    abstract ImmutableBTreePartition readSSTables(ImmutableBTreePartition fromMemtable, List<SSTableReader> sstables);

    public boolean onlyUnrepaired()
    {
        return onlyUnrepaired;
    }

    public int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    public ImmutableBTreePartition readPartition()
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey));

        ImmutableBTreePartition result = null;

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey);
            if (partition == null)
                continue;

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter, partition))
            {
                if (iter.isEmpty())
                    continue;

                result = add(iter, result, filter, false);
            }
        }

        Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
        return readSSTables(result, view.sstables);
    }

    protected ImmutableBTreePartition add(UnfilteredRowIterator iter,
                                        ImmutableBTreePartition result,
                                        ClusteringIndexNamesFilter filter,
                                        boolean isRepaired)
    {

        if (!isRepaired)
            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(columnFilter, Slices.ALL, filter.isReversed()))))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    boolean shouldInclude(SSTableReader sstable)
    {
        // If some static columns are queried, we should always include the sstable: the clustering values stats of the sstable
        // don't tell us if the sstable contains static values in particular.
        // TODO: we could record if a sstable contains any static value at all.
        if (!columnFilter.fetchedColumns().statics.isEmpty())
            return true;

        return filter.shouldInclude(sstable);
    }


    private static class WithoutRepairedDataTracking extends TimestampOrderedPartitionReader
    {
        private WithoutRepairedDataTracking(ColumnFamilyStore cfs,
                                    DecoratedKey partitionKey,
                                    ClusteringIndexNamesFilter filter,
                                    ColumnFilter columnFilter,
                                    SSTableReadsListener metricsCollector)
        {
            super(cfs, partitionKey, filter, columnFilter, metricsCollector);
        }

        ImmutableBTreePartition readSSTables(ImmutableBTreePartition result, List<SSTableReader> sstables)
        {
            // original logic from SinglePartitionReadCommand::queryMemtableAndSSTablesInTimestampOrder
            // with short circuits and no repaired data tracking
            for (SSTableReader sstable : sstables)
            {
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we're done, since the rest of the sstables
                // will also be older
                if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                    break;

                long currentMaxTs = sstable.getMaxTimestamp();
                filter = reduceFilter(filter, result, currentMaxTs);
                if (filter == null)
                    break;

                if (!shouldInclude(sstable))
                {
                    // This mean that nothing queried by the filter can be in the sstable. One exception is the top-level partition deletion
                    // however: if it is set, it impacts everything and must be included. Getting that top-level partition deletion costs us
                    // some seek in general however (unless the partition is indexed and is in the key cache), so we first check if the sstable
                    // has any tombstone at all as a shortcut.
                    if (!sstable.mayHaveTombstones())
                        continue; // no tombstone at all, we can skip that sstable

                    // We need to get the partition deletion and include it if it's live. In any case though, we're done with that sstable.
                    try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                           sstable,
                                                                                           partitionKey,
                                                                                           filter.getSlices(metadata),
                                                                                           columnFilter,
                                                                                           filter.isReversed(),
                                                                                           metricsCollector))
                    {
                        if (!iter.partitionLevelDeletion().isLive())
                            result = add(UnfilteredRowIterators.noRowsIterator(iter.metadata(), iter.partitionKey(), Rows.EMPTY_STATIC_ROW, iter.partitionLevelDeletion(), filter.isReversed()), result, filter, sstable.isRepaired());
                        else
                            result = add(iter, result, filter, sstable.isRepaired());
                    }
                    continue;
                }

                try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                       sstable,
                                                                                       partitionKey,
                                                                                       filter.getSlices(metadata),
                                                                                       columnFilter,
                                                                                       filter.isReversed(),
                                                                                       metricsCollector))
                {
                    if (iter.isEmpty())
                        continue;

                    if (sstable.isRepaired())
                        onlyUnrepaired = false;
                    result = add(iter, result, filter, sstable.isRepaired());
                }
            }
            return result;
        }

        private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp)
        {
            if (result == null)
                return filter;

            SearchIterator<Clustering, Row> searchIter = result.searchIterator(columnFilter, false);

            RegularAndStaticColumns columns = columnFilter.fetchedColumns();
            NavigableSet<Clustering> clusterings = filter.requestedRows();

            // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.
            // TODO: we could also remove a selected column if we've found values for every requested row but we'll leave
            // that for later.

            boolean removeStatic = false;
            if (!columns.statics.isEmpty())
            {
                Row staticRow = searchIter.next(Clustering.STATIC_CLUSTERING);
                removeStatic = staticRow != null && canRemoveRow(staticRow, columns.statics, sstableTimestamp);
            }

            NavigableSet<Clustering> toRemove = null;
            for (Clustering clustering : clusterings)
            {
                Row row = searchIter.next(clustering);
                if (row == null || !canRemoveRow(row, columns.regulars, sstableTimestamp))
                    continue;

                if (toRemove == null)
                    toRemove = new TreeSet<>(result.metadata().comparator);
                toRemove.add(clustering);
            }

            if (!removeStatic && toRemove == null)
                return filter;

            // Check if we have everything we need
            boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
            boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
            if (hasNoMoreStatic && hasNoMoreClusterings)
                return null;

            if (toRemove != null)
            {
                BTreeSet.Builder<Clustering> newClusterings = BTreeSet.builder(result.metadata().comparator);
                newClusterings.addAll(Sets.difference(clusterings, toRemove));
                clusterings = newClusterings.build();
            }
            return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
        }

        private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
        {
            // We can remove a row if it has data that is more recent that the next sstable to consider for the data that the query
            // cares about. And the data we care about is 1) the row timestamp (since every query cares if the row exists or not)
            // and 2) the requested columns.
            if (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp)
                return false;

            for (ColumnMetadata column : requestedColumns)
            {
                Cell cell = row.getCell(column);
                if (cell == null || cell.timestamp() <= sstableTimestamp)
                    return false;
            }
            return true;
        }

    }

    private static class WithRepairedDataTracking extends TimestampOrderedPartitionReader
    {
        private final RepairedDataInfo repairedDataInfo;

        private WithRepairedDataTracking(ColumnFamilyStore cfs,
                                 DecoratedKey partitionKey,
                                 ClusteringIndexNamesFilter filter,
                                 ColumnFilter columnFilter,
                                 SSTableReadsListener metricsCollector,
                                 RepairedDataInfo repairedDataInfo)
        {
            super(cfs, partitionKey, filter, columnFilter, metricsCollector);
            this.repairedDataInfo = repairedDataInfo;
        }

        ImmutableBTreePartition readSSTables(ImmutableBTreePartition result, List<SSTableReader> sstables)
        {
            List<UnfilteredRowIterator> unrepairedIterators = new ArrayList<>(sstables.size());
            List<UnfilteredRowIterator> repairedIterators = null;
            long mostRecentPartitionDelete = result == null
                                             ? Long.MIN_VALUE
                                             : Math.max(Long.MIN_VALUE, result.partitionLevelDeletion().markedForDeleteAt());
            try
            {
                for (SSTableReader sstable : sstables)
                {
                    // if we've already seen a partition tombstone with a timestamp greater
                    // than the most recent update to this sstable, we're done, since the rest of the sstables
                    // will also be older.
                    // as we're tracking repaired status, we mark the repaired digest inconclusive
                    // as other replicas may not have seen this partition delete and so could include
                    // data from this sstable in their digests
                    if (sstable.getMaxTimestamp() < mostRecentPartitionDelete)
                    {
                        if (considerRepairedForTracking(sstable))
                            repairedDataInfo.markInconclusive();

                        break;
                    }

                    // which collection of iterators does one we obtain from this sstable belong in?
                    List<UnfilteredRowIterator> iterList;
                    if (considerRepairedForTracking(sstable))
                    {
                        if (repairedIterators == null)
                            repairedIterators = new ArrayList<>(sstables.size());
                        iterList = repairedIterators;
                    }
                    else
                    {
                        iterList = unrepairedIterators;
                    }

                    if (!shouldInclude(sstable))
                    {
                        // This mean that nothing queried by the filter can be in the sstable. One exception is the top-level partition deletion
                        // however: if it is set, it impacts everything and must be included. Getting that top-level partition deletion costs us
                        // some seek in general however (unless the partition is indexed and is in the key cache), so we first check if the sstable
                        // has any tombstone at all as a shortcut.
                        if (!sstable.mayHaveTombstones())
                            continue; // no tombstone at all, we can skip that sstable

                        // We need to get the partition deletion and include it if it's live. In any case though, we're done with that sstable.
                        UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                          sstable,
                                                                                          partitionKey,
                                                                                          filter.getSlices(metadata),
                                                                                          columnFilter,
                                                                                          filter.isReversed(),
                                                                                          metricsCollector);
                        if (!iter.partitionLevelDeletion().isLive())
                        {
                            iterList.add(UnfilteredRowIterators.noRowsIterator(iter.metadata(),
                                                                               iter.partitionKey(),
                                                                               Rows.EMPTY_STATIC_ROW,
                                                                               iter.partitionLevelDeletion(),
                                                                               filter.isReversed()));
                        }
                        else
                        {
                            iterList.add(iter);
                        }

                        mostRecentPartitionDelete = Math.max(mostRecentPartitionDelete,
                                                             iter.partitionLevelDeletion().markedForDeleteAt());
                        continue;
                    }

                    @SuppressWarnings("resource") // both lists of iterators are closed in the finally
                    UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                                      sstable,
                                                                                      partitionKey,
                                                                                      filter.getSlices(metadata),
                                                                                      columnFilter,
                                                                                      filter.isReversed(),
                                                                                      metricsCollector);
                    if (iter.isEmpty())
                        continue;

                    if (sstable.isRepaired())
                        onlyUnrepaired = false;

                    mostRecentPartitionDelete = Math.max(mostRecentPartitionDelete,
                                                         iter.partitionLevelDeletion().markedForDeleteAt());
                    iterList.add(iter);
                }


                for (UnfilteredRowIterator iter : unrepairedIterators)
                    result = add(iter, result, filter, false);

                // merge any unrepaired iterators, wrapping with a digest generator
                if (repairedIterators != null)
                    result = add(UnfilteredRowIterators.withRepairedDataTracking(repairedDataInfo,
                                                                                 UnfilteredRowIterators.merge(repairedIterators)),
                                 result, filter, true);
            }
            finally
            {
                Iterable<UnfilteredRowIterator> toClose = repairedIterators == null
                                                          ? unrepairedIterators
                                                          : Iterables.concat(unrepairedIterators, repairedIterators);
                closeAll(toClose);
            }

            return result;
        }

        private void closeAll(Iterable<UnfilteredRowIterator> iterators)
        {
            Throwable t = null;
            for (UnfilteredRowIterator iter : iterators)
            {
                try
                {
                    iter.close();
                }
                catch (Throwable t2)
                {
                    if (t == null)
                    {
                        t = t2;
                    }
                    else
                    {
                        t.addSuppressed(t2);
                    }
                }
            }

            if (t != null)
            {
                Throwables.propagate(t);
            }
        }

        private boolean considerRepairedForTracking(SSTableReader sstable)
        {
            UUID pendingRepair = sstable.getPendingRepair();
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
            {
                if (ActiveRepairService.instance.consistent.local.isSessionFinalized(pendingRepair))
                    return true;
                else
                    repairedDataInfo.markInconclusive();
            }

            return sstable.isRepaired();
        }
    }
}

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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.function.LongPredicate;

import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.ByteBufferUtil;

class RepairedDataInfo
{
    public static final RepairedDataInfo NULL_REPAIRED_DATA_INFO = new RepairedDataInfo()
    {
        void trackPartitionKey(DecoratedKey key){}
        void trackDeletion(DeletionTime deletion){}
        void trackRangeTombstoneMarker(RangeTombstoneMarker marker){}
        void trackRow(Row row){}
        boolean isConclusive(){ return true; }
        ByteBuffer getDigest(){ return ByteBufferUtil.EMPTY_BYTE_BUFFER; }
    };

    // Keeps a digest of the partition currently being processed. Since we won't know
    // whether a partition will be fully purged from a read result until it's been
    // consumed, we buffer this per-partition digest and add it to the final digest
    // when the partition is closed (if it wasn't fully purged).
    private Digest perPartitionDigest;
    private Digest perCommandDigest;
    private boolean isConclusive = true;
    private ByteBuffer calculatedDigest = null;

    // Doesn't actually purge from the underlying iterators, but excludes from the digest
    // the purger can't be initialized until we've iterated all the sstables for the query
    // as it requires the oldest repaired tombstone
    private RepairedDataPurger purger;
    private boolean isFullyPurged = true;

    ByteBuffer getDigest()
    {
        if (calculatedDigest != null)
            return calculatedDigest;

        calculatedDigest = perCommandDigest == null
                           ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                           : ByteBuffer.wrap(perCommandDigest.digest());

        return calculatedDigest;
    }

    protected void onNewPartition(UnfilteredRowIterator partition)
    {
        assert purger != null;
        purger.setCurrentKey(partition.partitionKey());
        purger.setIsReverseOrder(partition.isReverseOrder());
        trackPartitionKey(partition.partitionKey());
    }

    protected void prepare(ColumnFamilyStore cfs, int nowInSec, int oldestUnrepairedTombstone)
    {
        this.purger= new RepairedDataPurger(cfs, nowInSec, oldestUnrepairedTombstone);
    }

    boolean isConclusive()
    {
        return isConclusive;
    }

    void markInconclusive()
    {
        isConclusive = false;
    }

    void trackPartitionKey(DecoratedKey key)
    {
        getPerPartitionDigest().update(key.getKey());
    }

    void trackDeletion(DeletionTime deletion)
    {
        assert purger != null;
        DeletionTime purged = purger.applyToDeletion(deletion);
        if (!purged.isLive())
            isFullyPurged = false;

        purged.digest(getPerPartitionDigest());
    }

    void trackRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        assert purger != null;
        RangeTombstoneMarker purged = purger.applyToMarker(marker);
        if (purged != null)
        {
            isFullyPurged = false;
            purged.digest(getPerPartitionDigest());
        }
    }

    void trackStaticRow(Row row)
    {
        assert purger != null;
        Row purged = purger.applyToRow(row);
        if (!purged.isEmpty())
        {
            isFullyPurged = false;
            purged.digest(getPerPartitionDigest());
        }
    }

    void trackRow(Row row)
    {
        assert purger != null;
        Row purged = purger.applyToRow(row);
        if (purged != null)
        {
            isFullyPurged = false;
            purged.digest(getPerPartitionDigest());
        }
    }

    private Digest getPerPartitionDigest()
    {
        if (perPartitionDigest == null)
            perPartitionDigest = Digest.forRepairedDataTracking();

        return perPartitionDigest;
    }

    private void onPartitionClose()
    {
        if (perPartitionDigest != null)
        {
            // If the partition wasn't completely emptied by the purger,
            // calculate the digest for the partition and use it to
            // update the overall digest
            if (!isFullyPurged)
            {
                if (perCommandDigest == null)
                    perCommandDigest = Digest.forRepairedDataTracking();

                byte[] partitionDigest = perPartitionDigest.digest();
                perCommandDigest.update(partitionDigest, 0, partitionDigest.length);
                isFullyPurged = true;
            }

            perPartitionDigest = null;
        }
    }

    public static UnfilteredPartitionIterator withRepairedDataInfo(final UnfilteredPartitionIterator iterator,
                                                                   final RepairedDataInfo repairedDataInfo)
    {
        class WithRepairedDataTracking extends Transformation<UnfilteredRowIterator>
        {
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return withRepairedDataInfo(partition, repairedDataInfo);
            }
        }

        return Transformation.apply(iterator, new WithRepairedDataTracking());
    }

    public static UnfilteredRowIterator withRepairedDataInfo(final UnfilteredRowIterator iterator,
                                                             final RepairedDataInfo repairedDataInfo)
    {
        class WithTracking extends Transformation
        {
            protected DecoratedKey applyToPartitionKey(DecoratedKey key)
            {
                repairedDataInfo.trackPartitionKey(key);
                return key;
            }

            protected DeletionTime applyToDeletion(DeletionTime deletionTime)
            {
                repairedDataInfo.trackDeletion(deletionTime);
                return deletionTime;
            }

            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                repairedDataInfo.trackRangeTombstoneMarker(marker);
                return marker;
            }

            protected Row applyToStatic(Row row)
            {
                repairedDataInfo.trackStaticRow(row);
                return row;
            }

            protected Row applyToRow(Row row)
            {
                repairedDataInfo.trackRow(row);
                return row;
            }

            protected void onPartitionClose()
            {
                repairedDataInfo.onPartitionClose();
            }
        }
        repairedDataInfo.onNewPartition(iterator);
        return Transformation.apply(iterator, new WithTracking());
    }

    /**
     * Although PurgeFunction extends Transformation, this is never applied to an iterator.
     * Instead, it is used by RepairedDataInfo during the generation of a repaired data
     * digest to exclude data which will actually be purged later on in the read pipeline.
     */
    private static class RepairedDataPurger extends PurgeFunction
    {
        RepairedDataPurger(ColumnFamilyStore cfs,
                           int nowInSec,
                           int oldestUnrepairedTombstone)
        {
            super(nowInSec,
                  cfs.gcBefore(nowInSec),
                  oldestUnrepairedTombstone,
                  cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                  cfs.metadata.get().enforceStrictLiveness());
        }

        protected LongPredicate getPurgeEvaluator()
        {
            return (time) -> true;
        }

        void setCurrentKey(DecoratedKey key)
        {
            super.onNewPartition(key);
        }

        void setIsReverseOrder(boolean isReverseOrder)
        {
            super.setReverseOrder(isReverseOrder);
        }

        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            return super.applyToDeletion(deletionTime);
        }

        public Row applyToRow(Row row)
        {
            return super.applyToRow(row);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return super.applyToMarker(marker);
        }
    }
}

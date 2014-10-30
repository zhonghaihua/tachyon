package tachyon.r.sorted;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import tachyon.thrift.NetAddress;
import tachyon.thrift.SortedStorePartitionInfo;
import tachyon.util.CommonUtils;

/**
 * This is one type of partition.
 */
public class MasterPartition {
  public final int mStoreID;
  public final int mPartitionIndex;
  public final int mDataFileID;
  public final int mIndexFileID;

  public final ByteBuffer mStartKey;
  public final ByteBuffer mEndKey;

  private Set<NetAddress> mLocations = new HashSet<NetAddress>();

  MasterPartition(int storeId, int partitionIndex, int dataFileId, int indexFileId,
      ByteBuffer start, ByteBuffer end) {
    mStoreID = storeId;
    mPartitionIndex = partitionIndex;
    mDataFileID = dataFileId;
    mIndexFileID = indexFileId;

    mStartKey = CommonUtils.cloneByteBuffer(start);
    mEndKey = CommonUtils.cloneByteBuffer(end);
  }

  public synchronized void addLocation(NetAddress address) {
    mLocations.add(address);
  }

  public synchronized boolean containsLocation(NetAddress address) {
    return mLocations.contains(address);
  }

  public synchronized SortedStorePartitionInfo generateSortedStorePartitionInfo() {
    SortedStorePartitionInfo res = new SortedStorePartitionInfo();
    res.setStoreId(mStoreID);
    res.setPartitionIndex(mPartitionIndex);
    res.setDataFileId(mDataFileID);
    res.setIndexFileId(mIndexFileID);
    res.setStartKey(mStartKey.array());
    res.setEndKey(mEndKey.array());
    if (mLocations.size() > 0) {
      res.setLocation(mLocations.iterator().next());
    }
    return res;
  }

  public synchronized boolean hasLocation() {
    return !mLocations.isEmpty();
  }

  public synchronized void removeLocation(NetAddress address) {
    mLocations.remove(address);
  }

  @Override
  public String toString() {
    return new StringBuilder("MasterPartition(").append("STORE_ID ").append(mStoreID)
        .append(", PARTITION_INDEX ").append(mPartitionIndex).append(", DATA_FILE_ID ")
        .append(mDataFileID).append(", INDEX_FILE_ID ").append(mIndexFileID)
        .append(", START_KEY ").append(mStartKey).append(", END_KEY ").append(mEndKey).append(")")
        .toString();
  }
}

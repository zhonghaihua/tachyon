package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;

/**
 * Metadata of a key/value store in the master.
 */
public class MasterStore {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public final int mInodeID;

  private List<MasterPartition> mPartitions = new ArrayList<MasterPartition>();

  public MasterStore(int inodeId) {
    mInodeID = inodeId;
  }

  void addPartition(MasterPartition partition) throws IOException {
    while (mPartitions.size() <= partition.mPartitionIndex) {
      mPartitions.add(null);
    }

    if (mPartitions.get(partition.mPartitionIndex) != null) {
      throw new IOException("Partition has been added before: " + partition);
    }

    mPartitions.set(partition.mPartitionIndex, partition);

    for (int k = partition.mPartitionIndex - 1; k >= 0; k --) {
      if (mPartitions.get(k) != null) {
        if (Utils.compare(mPartitions.get(k).mEndKey, partition.mStartKey) > 1) {
          throw new IOException("Wrong partition order: " + mPartitions.get(k) + " > " + partition);
        }
        break;
      }
    }

    for (int k = partition.mPartitionIndex + 1; k < mPartitions.size(); k ++) {
      if (mPartitions.get(k) != null) {
        if (Utils.compare(partition.mEndKey, mPartitions.get(k).mStartKey) > 1) {
          throw new IOException("Wrong partition order: " + mPartitions.get(k) + " < " + partition);
        }
        break;
      }
    }
  }

  MasterPartition getPartition(ByteBuffer buf) {
    // TODO Make this method efficient.
    for (int k = 0; k < mPartitions.size(); k ++) {
      MasterPartition partition = mPartitions.get(k);
      if (null == partition) {
        LOG.warn("KVStore " + mInodeID + " has null partition when being queried.");
        continue;
      }
      LOG.info("GetPartition: " + partition + " " + buf + " "
          + Utils.byteArrayToString(partition.mStartKey.array()) + " "
          + Utils.byteArrayToString(buf.array()) + " "
          + Utils.byteArrayToString(partition.mEndKey.array()));
      if (Utils.compare(partition.mStartKey, buf) <= 0
          && Utils.compare(partition.mEndKey, buf) >= 0) {
        return partition;
      }
    }
    return null;
  }

  MasterPartition getPartition(int partitionIndex) {
    return mPartitions.get(partitionIndex);
  }
}

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
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public final int INODE_ID;

  private List<MasterPartition> mPartitions = new ArrayList<MasterPartition>();

  public MasterStore(int inodeId) {
    INODE_ID = inodeId;
  }

  void addPartition(MasterPartition partition) throws IOException {
    while (mPartitions.size() <= partition.PARTITION_INDEX) {
      mPartitions.add(null);
    }

    if (mPartitions.get(partition.PARTITION_INDEX) != null) {
      throw new IOException("Partition has been added before: " + partition);
    }

    mPartitions.set(partition.PARTITION_INDEX, partition);

    for (int k = partition.PARTITION_INDEX - 1; k >= 0; k --) {
      if (mPartitions.get(k) != null) {
        if (Utils.compare(mPartitions.get(k).END_KEY, partition.START_KEY) > 1) {
          throw new IOException("Wrong partition order: " + mPartitions.get(k) + " > " + partition);
        }
        break;
      }
    }

    for (int k = partition.PARTITION_INDEX + 1; k < mPartitions.size(); k ++) {
      if (mPartitions.get(k) != null) {
        if (Utils.compare(partition.END_KEY, mPartitions.get(k).START_KEY) > 1) {
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
        LOG.warn("KVStore " + INODE_ID + " has null partition when being queried.");
        continue;
      }
      LOG.info("GetPartition: " + partition + " " + buf + " "
          + Utils.byteArrayToString(partition.START_KEY.array()) + " "
          + Utils.byteArrayToString(buf.array()) + " "
          + Utils.byteArrayToString(partition.END_KEY.array()));
      if (Utils.compare(partition.START_KEY, buf) <= 0
          && Utils.compare(partition.END_KEY, buf) >= 0) {
        return partition;
      }
    }
    return null;
  }

  MasterPartition getPartition(int partitionIndex) {
    return mPartitions.get(partitionIndex);
  }
}

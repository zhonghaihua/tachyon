package tachyon.r.sorted.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.SortedStorePartitionInfo;
import tachyon.thrift.TachyonException;

/**
 * All key/value stores information in the master;
 */
public class StoresInfo {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private Map<Integer, StoreInfo> mStores = new HashMap<Integer, StoreInfo>();

  public synchronized void addStoresInfo(StoreInfo info) throws FileAlreadyExistException {
    if (mStores.containsKey(info.INODE_ID)) {
      throw new FileAlreadyExistException("The store already exists: " + info);
    }

    mStores.put(info.INODE_ID, info);
  }

  public synchronized boolean addPartition(SortedStorePartitionInfo info) throws TachyonException {
    int storeId = info.storeId;
    if (!mStores.containsKey(storeId)) {
      throw new TachyonException("Store does not exist for partition: " + info);
    }
    try {
      mStores.get(storeId).addPartition(
          new MasterPartition(info.storeId, info.partitionIndex, info.dataFileId,
              info.indexFileId, info.startKey, info.endKey));
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw new TachyonException(e.getMessage());
    }
    return true;
  }

  public synchronized MasterPartition get(int storeId, ByteBuffer key) throws TachyonException {
    if (!mStores.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return mStores.get(storeId).getPartition(key);
  }

  public synchronized MasterPartition get(int storeId, int partitionIndex) throws TachyonException {
    if (!mStores.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return mStores.get(storeId).getPartition(partitionIndex);
  }
}

package tachyon.r.sorted.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.PartitionSortedStorePartitionInfo;
import tachyon.thrift.TachyonException;

/**
 * All key/value stores information in the master;
 */
public class StoresInfo {
  public Map<Integer, StoreInfo> KVS = new HashMap<Integer, StoreInfo>();

  public synchronized void addStoresInfo(StoreInfo info) throws FileAlreadyExistException {
    if (KVS.containsKey(info.INODE_ID)) {
      throw new FileAlreadyExistException("The store already exists: " + info);
    }

    KVS.put(info.INODE_ID, info);
  }

  public synchronized boolean addPartition(PartitionSortedStorePartitionInfo info)
      throws TachyonException, IOException {
    int storeId = info.storeId;
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist for partition: " + info);
    }
    KVS.get(storeId).addPartition(
        new MasterPartition(info.storeId, info.partitionIndex, info.dataFileId, info.indexFileId,
            info.startKey, info.endKey));
    return true;
  }

  public synchronized MasterPartition get(int storeId, ByteBuffer key) throws TachyonException {
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return KVS.get(storeId).getPartition(key);
  }

  public synchronized MasterPartition get(int storeId, int partitionIndex) throws TachyonException {
    if (!KVS.containsKey(storeId)) {
      throw new TachyonException("Store does not exist: " + storeId);
    }
    return KVS.get(storeId).getPartition(partitionIndex);
  }
}

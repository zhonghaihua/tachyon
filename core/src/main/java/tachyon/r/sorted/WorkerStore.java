package tachyon.r.sorted;

import java.io.IOException;
import java.util.HashMap;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.r.WorkerStoreBase;
import tachyon.thrift.PartitionSortedStorePartitionInfo;
import tachyon.util.CommonUtils;

public class WorkerStore extends WorkerStoreBase {
  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;

  // Mapping: <storeId, <partitionIndex, Partition>>
  private HashMap<Integer, HashMap<Integer, WorkerPartition>> mData;

  WorkerStore(TachyonURI uri) throws IOException {
    super(uri);
    mTFS = TachyonFS.get(URI.toString());
    mData = new HashMap<Integer, HashMap<Integer, WorkerPartition>>();
  }

  @Override
  public byte[] get(byte[] key) {
    CommonUtils.runtimeException("PartitionSortedWorkerStore get");
    return null;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    CommonUtils.runtimeException("PartitionSortedWorkerStore put");
  }

  public byte[] get(PartitionSortedStorePartitionInfo info, byte[] key) throws IOException {
    if (!mData.containsKey(info.getStoreId())) {
      mData.put(info.getStoreId(), new HashMap<Integer, WorkerPartition>());
    }

    HashMap<Integer, WorkerPartition> store = mData.get(info.getStoreId());
    if (!store.containsKey(info.getPartitionIndex())) {
      store.put(info.getPartitionIndex(), new WorkerPartition(mTFS, info));
    }

    return store.get(info.getPartitionIndex()).get(key);
  }
}

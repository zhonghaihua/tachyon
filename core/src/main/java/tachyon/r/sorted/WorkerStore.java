package tachyon.r.sorted;

import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.r.WorkerStoreBase;
import tachyon.thrift.SortedStorePartitionInfo;

public class WorkerStore extends WorkerStoreBase {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;

  // Mapping: <storeId, <partitionIndex, Partition>>
  private HashMap<Integer, HashMap<Integer, WorkerPartition>> mData;

  public WorkerStore(TachyonURI uri) throws IOException {
    super(uri);
    LOG.info(URI.toString());
    mTFS = TachyonFS.get(URI.toString());
    LOG.info("Good");
    mData = new HashMap<Integer, HashMap<Integer, WorkerPartition>>();
  }

  @Override
  public byte[] get(byte[] key) {
    throw new RuntimeException("PartitionSortedWorkerStore get");
  }

  @Override
  public void put(byte[] key, byte[] value) {
    throw new RuntimeException("PartitionSortedWorkerStore put");
  }

  public byte[] get(SortedStorePartitionInfo info, byte[] key) throws IOException {
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

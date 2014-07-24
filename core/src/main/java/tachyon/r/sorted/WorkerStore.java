package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.r.WorkerStoreBase;
import tachyon.thrift.PartitionSortedStorePartitionInfo;

public class WorkerStore extends WorkerStoreBase {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;

  // Mapping: <storeId, <partitionIndex, Partition>>
  // private HashMap<Integer, HashMap<Integer, WorkerPartition>> mData;
  private HashMap<Integer, TachyonByteBuffer> mData;

  public WorkerStore(TachyonURI uri) throws IOException {
    super(uri);
    LOG.info(URI.toString());
    mTFS = TachyonFS.get(URI.toString());
    LOG.info("Good");
    mData = new HashMap<Integer, TachyonByteBuffer>();
    // mData = new HashMap<Integer, HashMap<Integer, WorkerPartition>>();
  }

  @Override
  public byte[] get(byte[] key) {
    throw new RuntimeException("PartitionSortedWorkerStore get");
  }

  @Override
  public void put(byte[] key, byte[] value) {
    throw new RuntimeException("PartitionSortedWorkerStore put");
  }

  // public byte[] get(PartitionSortedStorePartitionInfo info, byte[] key) throws IOException {
  // if (!mData.containsKey(info.getStoreId())) {
  // mData.put(info.getStoreId(), new HashMap<Integer, WorkerPartition>());
  // }
  //
  // HashMap<Integer, WorkerPartition> store = mData.get(info.getStoreId());
  // if (!store.containsKey(info.getPartitionIndex())) {
  // store.put(info.getPartitionIndex(), new WorkerPartition(mTFS, info));
  // }
  //
  // return store.get(info.getPartitionIndex()).get(key);
  // }

  public ByteBuffer get(PartitionSortedStorePartitionInfo info, ByteBuffer key) throws IOException {
    validate(info.getDataFileId());
    validate(info.getIndexFileId());

    TachyonByteBuffer dataBuffer = mData.get(info.getDataFileId());
    TachyonByteBuffer indexBuffer = mData.get(info.getIndexFileId());

    ByteBuffer result = null;

    ByteBuffer data = dataBuffer.DATA;
    // HashMap<byte[], byte[]> kv = new HashMap<byte[], byte[]>();
    while (data.hasRemaining()) {
      int size = data.getInt();
      byte[] tKey = new byte[size];
      data.get(tKey);
      size = data.getInt();
      byte[] tValue = new byte[size];
      data.get(tValue);
      if (Utils.compare(tKey, key.array()) == 0) {
        result = ByteBuffer.allocate(tValue.length);
        result.put(tValue);
        result.flip();
        break;
      }
    }
    dataBuffer.DATA.clear();

    if (null == result) {
      // This should return "null". But Thrift does not handle "null" well.
      return ByteBuffer.allocate(0);
    }

    LOG.info("===========Getting result " + info + " " + key + " " + result);
    return result;
  }

  private synchronized void validate(int fileId) throws IOException {
    if (mData.containsKey(fileId)) {
      return;
    }
    TachyonFile file = mTFS.getFile(fileId);
    mData.put(fileId, file.readByteBuffer());
  }
}

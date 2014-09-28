package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.collect.ImmutableList;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.extension.ComponentException;
import tachyon.extension.WorkerComponent;
import tachyon.thrift.SortedStorePartitionInfo;

public class SortedKVWorkerStore extends WorkerComponent {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final TachyonURI URI;

  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;

  // Mapping: <storeId, <partitionIndex, Partition>>
  private HashMap<Integer, HashMap<Integer, WorkerPartition>> mData;

  public SortedKVWorkerStore(TachyonURI uri) throws IOException {
    URI = uri;
    LOG.info(URI.toString());
    mTFS = TachyonFS.get(URI);
    mData = new HashMap<Integer, HashMap<Integer, WorkerPartition>>();
  }

  private byte[] get(SortedStorePartitionInfo info, byte[] key) throws IOException {
    if (!mData.containsKey(info.getStoreId())) {
      mData.put(info.getStoreId(), new HashMap<Integer, WorkerPartition>());
    }

    HashMap<Integer, WorkerPartition> store = mData.get(info.getStoreId());
    if (!store.containsKey(info.getPartitionIndex())) {
      store.put(info.getPartitionIndex(), new WorkerPartition(mTFS, info));
    }

    return store.get(info.getPartitionIndex()).get(key);
  }

  @Override
  public List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException {
    minLengthCheck(data);

    WorkerOperationType opType = null;
    try {
      opType = WorkerOperationType.getOpType(data.get(0));
    } catch (IOException e) {
      throw new ComponentException(e);
    }

    try {
      switch (opType) {
      case GET: {
        lengthCheck(data, 3, opType.toString());

        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        SortedStorePartitionInfo info = new SortedStorePartitionInfo();
        deserializer.deserialize(info, data.get(1).array());

        return ImmutableList.of(ByteBuffer.wrap(get(info, data.get(2).array())));
      }
      }
    } catch (TException e) {
      LOG.error(e.getMessage(), e);
      throw new ComponentException(e);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new ComponentException(e);
    }

    throw new ComponentException("Unprocessed WorkerOperationType " + opType);
  }
}

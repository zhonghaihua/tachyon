package tachyon.r.sorted;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.collect.ImmutableList;

import tachyon.TachyonURI;
import tachyon.r.ClientStoreBase;
import tachyon.thrift.SortedStorePartitionInfo;

public class ClientStore extends ClientStoreBase {
  public static ClientStore createStore(TachyonURI uri) throws IOException {
    return new ClientStore(uri, true);
  }

  public static ClientStore getStore(TachyonURI uri) throws IOException {
    return new ClientStore(uri, false);
  }

  /** the map from partition id to the ClientPartition */
  private Map<Integer, ClientPartition> mWritePartitions = Collections
      .synchronizedMap(new HashMap<Integer, ClientPartition>());

  /** the map from partition id to the SortedStorePartitionInfo */
  private Map<Integer, SortedStorePartitionInfo> mReadPartitions = Collections
      .synchronizedMap(new HashMap<Integer, SortedStorePartitionInfo>());

  protected ClientStore(TachyonURI uri, boolean create) throws IOException {
    super(uri, "tachyon.r.sorted.shard", create);
  }

  public void closePartition(int partitionId) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }
    mWritePartitions.get(partitionId).close();
    mWritePartitions.remove(partitionId);
  }

  public void createPartition(int partitionId) throws IOException {
    if (mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has been created before");
    }

    mWritePartitions.put(partitionId, ClientPartition.createPartitionSortedStorePartition(
        mTachyonFS, mID, mURI.getPath(), partitionId));
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    List<Integer> pIds = lookup(key);
    if (pIds.size() == 0) {
      return null;
    }
    if (pIds.size() > 1) {
      throw new IOException("More than one partition containing the key;");
    }

    SortedStorePartitionInfo info = mReadPartitions.get(pIds.get(0));

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    byte[] bytes = null;
    try {
      bytes = serializer.serialize(info);
    } catch (TException e) {
      throw new IOException(e);
    }

    InetSocketAddress workerAddress =
        new InetSocketAddress(info.location.mHost, info.location.mPort);
    byte[] result =
        mTachyonFS
            .workerProcess(
                workerAddress,
                ImmutableList.of(WorkerOperationType.GET.toByteBuffer(), ByteBuffer.wrap(bytes),
                    ByteBuffer.wrap(key))).get(0).array();
    return result.length == 0 ? null : result;
  }

  @Override
  public List<Integer> lookup(byte[] key) throws IOException {
    ByteBuffer tKey = ByteBuffer.wrap(key);
    List<Integer> res = new ArrayList<Integer>();
    for (Entry<Integer, SortedStorePartitionInfo> entry : mReadPartitions.entrySet()) {
      if (Utils.compare(entry.getValue().startKey, tKey) <= 0
          && Utils.compare(entry.getValue().endKey, tKey) >= 0) {
        res.add(entry.getKey());
      }
    }
    if (res.size() == 0) {
      ByteBuffer storeId = ByteBuffer.allocate(4);
      storeId.putInt(mID);
      storeId.flip();

      List<ByteBuffer> tmp =
          mTachyonFS.masterProcess(ImmutableList.of(
              MasterOperationType.GET_PARTITION.toByteBuffer(), storeId, ByteBuffer.wrap(key)));

      TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      SortedStorePartitionInfo info = new SortedStorePartitionInfo();
      try {
        deserializer.deserialize(info, tmp.get(0).array());
      } catch (TException e) {
        throw new IOException(e);
      }

      if (info.partitionIndex != -1) {
        mReadPartitions.put(info.partitionIndex, info);
        res.add(info.partitionIndex);
      }
    }
    return res;
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    throw new RuntimeException("The method has not been implemented yet");
  }

  public void put(int partitionId, byte[] key, byte[] value) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).put(key, value);
  }

  public void put(int partitionId, long key, long value) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).put(String.valueOf(key).getBytes(),
        String.valueOf(value).getBytes());
  }

  public void put(int partitionId, String key, int value) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).put(key.getBytes(), String.valueOf(value).getBytes());
  }

  public void put(int partitionId, String key, String value) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).put(key.getBytes(), value.getBytes());
  }
}

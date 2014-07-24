package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import tachyon.TachyonURI;
import tachyon.r.ClientStoreBase;
import tachyon.thrift.PartitionSortedStorePartitionInfo;

public class ClientStore extends ClientStoreBase {
  /** the map from partition id to the ClientPartition */
  private Map<Integer, ClientPartition> mWritePartitions = Collections
      .synchronizedMap(new HashMap<Integer, ClientPartition>());

  /** the map from partition id to the PartitionSortedStorePartitionInfo */
  private Map<Integer, PartitionSortedStorePartitionInfo> mReadPartitions = Collections
      .synchronizedMap(new HashMap<Integer, PartitionSortedStorePartitionInfo>());

  protected ClientStore(TachyonURI uri, boolean create) throws IOException {
    super(uri, "tachyon.r.sorted.shard", create);
  }

  public static ClientStore getStore(TachyonURI uri) throws IOException {
    return new ClientStore(uri, false);
  }

  public static ClientStore createStore(TachyonURI uri) throws IOException {
    return new ClientStore(uri, true);
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

    PartitionSortedStorePartitionInfo info = mReadPartitions.get(pIds.get(0));

    return mTachyonFS.r_get(info, key);
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    throw new RuntimeException("The method has not been implemented yet");
  }

  public void createPartition(int partitionId) throws IOException {
    if (mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has been created before");
    }

    mWritePartitions.put(partitionId, ClientPartition.createPartitionSortedStorePartition(
        mTachyonFS, ID, URI.getPath(), partitionId));
  }

  public void put(int partitionId, byte[] key, byte[] value) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).put(key, value);
  }

  public void closePartition(int partitionId) throws IOException {
    if (!mWritePartitions.containsKey(partitionId)) {
      throw new IOException("Partition " + partitionId + " has not been created yet.");
    }

    mWritePartitions.get(partitionId).close();
    mWritePartitions.remove(partitionId);
  }

  @Override
  public List<Integer> lookup(byte[] key) {
    ByteBuffer tKey = ByteBuffer.wrap(key);
    List<Integer> res = new ArrayList<Integer>();
    for (Entry<Integer, PartitionSortedStorePartitionInfo> entry : mReadPartitions.entrySet()) {
      if (Utils.compare(entry.getValue().startKey, tKey) <= 0
          && Utils.compare(entry.getValue().endKey, tKey) >= 0) {
        res.add(entry.getKey());
      }
    }
    if (res.size() == 0) {
      PartitionSortedStorePartitionInfo info = mTachyonFS.r_getPartition(ID, key);
      if (info.partitionIndex != -1) {
        mReadPartitions.put(info.partitionIndex, info);
        res.add(info.partitionIndex);
      }
    }
    return res;
  }
}

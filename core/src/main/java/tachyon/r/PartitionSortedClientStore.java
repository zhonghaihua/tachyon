package tachyon.r;

import java.io.IOException;

import tachyon.TachyonURI;

public class PartitionSortedClientStore extends ClientStoreBase {

  protected PartitionSortedClientStore(TachyonURI uri, ShardBase shard, boolean create)
      throws IOException {
    super(uri, shard, "tachyon.r.PartitionSortedClientStore", create);
  }

  public static PartitionSortedClientStore getStore(TachyonURI uri) throws IOException {
    return new PartitionSortedClientStore(uri, new PartitionSortedShard(false), false);
  }

  public static PartitionSortedClientStore createStore(TachyonURI uri) throws IOException {
    return new PartitionSortedClientStore(uri, new PartitionSortedShard(false), true);
  }

  @Override
  public byte[] get(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    // TODO Auto-generated method stub

  }

  public void createPartition(int partitionId) {
    mTachyonFS.r_createPartition(partitionId);
  }

  public void put(int partitionId, byte[] key, byte[] value) {

  }

  public void closePartition(int partitionId) {

  }
}

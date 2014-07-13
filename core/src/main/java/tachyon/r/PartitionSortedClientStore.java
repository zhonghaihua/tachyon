package tachyon.r;

public class PartitionSortedClientStore extends ClientStoreBase {

  protected PartitionSortedClientStore(String uri, ShardBase shard) {
    super(uri, shard);
    // TODO Auto-generated constructor stub
  }

  public static PartitionSortedClientStore getStore(String uri) {
    // TODO Auto-generated method stub
    return null;
  }

  public static PartitionSortedClientStore createStore(String uri) {
    // TODO Auto-generated method stub
    return null;
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

  public boolean createPartition(int partitionId) {
    return false;
  }

  public void put(int partitionId, byte[] key, byte[] value) {

  }

  public void closePartition(int partitionId) {

  }
}

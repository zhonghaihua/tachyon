package tachyon.r;

public abstract class ClientStoreBase extends StoreBase {
  protected ShardBase mShard;

  protected ClientStoreBase(String uri, ShardBase shard) {
    super(uri);

    mShard = shard;
  }
}

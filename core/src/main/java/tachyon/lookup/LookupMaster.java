package tachyon.lookup;

import tachyon.thrift.HashPartition;
import tachyon.thrift.NetAddress;

public interface LookupMaster {
  HashPartition lookup(byte[] key, int version);

  void updateNode(NetAddress location, boolean add);

  boolean updatePartition(HashPartition partition, boolean add);
}

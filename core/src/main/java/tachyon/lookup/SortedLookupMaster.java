package tachyon.lookup;

import tachyon.thrift.HashPartition;
import tachyon.thrift.NetAddress;

public class SortedLookupMaster implements LookupMaster {

  @Override
  public HashPartition lookup(byte[] key, int version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateNode(NetAddress location, boolean add) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean updatePartition(HashPartition partition, boolean add) {
    // TODO Auto-generated method stub
    return false;
  }

}

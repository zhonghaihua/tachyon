package tachyon.lookup;

import tachyon.thrift.HashPartition;

public interface LookupClient {
  HashPartition lookup(byte[] key, boolean useCache);
}

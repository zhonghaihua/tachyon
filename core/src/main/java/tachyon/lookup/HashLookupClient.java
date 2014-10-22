package tachyon.lookup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.thrift.HashPartition;

public class HashLookupClient implements LookupClient {
  private Set<HashPartition> mCachedPartitions = new HashSet<HashPartition>();
  // TODO: This should be an RPC client.
  private LookupMaster mMaster = new HashLookupMaster();

  @Override
  public synchronized HashPartition lookup(byte[] key, boolean useCache) {
    int value = Utils.hash(key);
    if (useCache) {
      for (HashPartition p : mCachedPartitions) {
        if (p.left <= value && value < p.right) {
          return p;
        }
      }
    }

    HashPartition ret = mMaster.lookup(key, -1);

    if (ret.pId == -1) {
      return null;
    }

    List<HashPartition> toBeRemoved = new ArrayList<HashPartition>();

    for (HashPartition p : mCachedPartitions) {
      if (p.left < ret.right && ret.left < p.right) {
        toBeRemoved.add(p);
      }
    }

    mCachedPartitions.removeAll(toBeRemoved);
    mCachedPartitions.add(ret);

    return ret;
  }

}

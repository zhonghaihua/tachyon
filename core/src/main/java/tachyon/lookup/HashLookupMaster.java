package tachyon.lookup;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import tachyon.thrift.HashPartition;
import tachyon.thrift.NetAddress;

public class HashLookupMaster implements LookupMaster {
  private Map<Integer, HashPartition> mPartitions = new HashMap<Integer, HashPartition>();
  private Map<NetAddress, Integer> mLocations = new HashMap<NetAddress, Integer>();

  @Override
  public synchronized HashPartition lookup(byte[] key, int version) {
    int value = Utils.hash(key);

    HashPartition ret = null;

    for (HashPartition p : mPartitions.values()) {
      if (p.left <= value && value < p.right) {
        ret = p;
      }
    }

    if (ret.location == null) {
      Entry<NetAddress, Integer> dest = null;
      for (Entry<NetAddress, Integer> entry : mLocations.entrySet()) {
        if (dest == null || dest.getValue() > entry.getValue()) {
          dest = entry;
        }
      }
      // TODO Log this.
      ret.location = dest.getKey();
      ret.version ++;
      dest.setValue(dest.getValue() + 1);
    }

    if (version == -1 || version < ret.version) {
      return ret;
    } else {
      // TODO Do fast location change
      return ret;
    }
  }

  @Override
  public synchronized void updateNode(NetAddress location, boolean add) {
    if (add) {
      if (!mLocations.containsKey(location)) {
        mLocations.put(location, 0);
      }
    } else {
      if (mLocations.containsKey(location)) {
        for (HashPartition p : mPartitions.values()) {
          if (p.location.equals(location)) {
            p.location = null;
          }
        }
      }
      mLocations.remove(location);
    }
  }

  // TODO make this a list; handle race condition
  @Override
  public synchronized boolean updatePartition(HashPartition partition, boolean add) {
    if (add) {
      if (mPartitions.containsKey(partition.pId)) {
        if (mPartitions.get(partition.pId).version < partition.version) {
          mPartitions.put(partition.pId, partition);
          return true;
        } else {
          return false;
        }
      } else {
        mPartitions.put(partition.pId, partition);
        return true;
      }
    } else {
      mPartitions.remove(partition);
      return true;
    }
  }
}

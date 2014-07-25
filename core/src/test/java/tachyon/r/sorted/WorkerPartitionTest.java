package tachyon.r.sorted;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.SortedStorePartitionInfo;

public class WorkerPartitionTest extends WorkerPartition {

  public WorkerPartitionTest() {
    super(null, new SortedStorePartitionInfo());
  }

  @Test
  public void getFromSortedDataTest() {
    WorkerPartitionTest workerPartitionTest = new WorkerPartitionTest();

    int limit = 100;
    int[] index = new int[limit];
    ByteBuffer data = ByteBuffer.allocate(limit * 16);
    for (int k = 0; k < limit; k ++) {
      index[k] = k * 16;
      data.putInt(4);
      data.putInt(k);
      data.putInt(4);
      data.putInt(k * k);
    }

    for (int k = 0; k < limit; k ++) {
      ByteBuffer key = ByteBuffer.allocate(4);
      key.putInt(k);
      ByteBuffer result =
          ByteBuffer.wrap(workerPartitionTest.getFromSortedData(key.array(), index, data));
      Assert.assertEquals(k * k, result.getInt());
    }
  }
}

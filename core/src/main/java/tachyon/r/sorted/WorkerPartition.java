package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.io.Utils;
import tachyon.thrift.SortedStorePartitionInfo;

public class WorkerPartition {
  private TachyonFS mTFS;
  private SortedStorePartitionInfo mInfo;
  private TachyonByteBuffer mData = null;
  private TachyonByteBuffer mIndex = null;
  private long mLastAccessTimeMs = System.currentTimeMillis();

  public WorkerPartition(TachyonFS tfs, SortedStorePartitionInfo partitionInfo) {
    mTFS = tfs;
    mInfo = new SortedStorePartitionInfo(partitionInfo);
  }

  public synchronized byte[] get(byte[] key) throws IOException {
    mLastAccessTimeMs = System.currentTimeMillis();
    if (null == mData) {
      TachyonFile file = mTFS.getFile(mInfo.getDataFileId());
      mData = file.readByteBuffer();
    }
    if (null == mIndex) {
      TachyonFile file = mTFS.getFile(mInfo.getIndexFileId());
      mIndex = file.readByteBuffer();
    }

    // TODO Correctness first, fix the efficiency later;
    byte[] result = new byte[0];
    ByteBuffer data = mData.DATA;
    while (data.hasRemaining()) {
      int size = data.getInt();
      byte[] tKey = new byte[size];
      data.get(tKey);
      size = data.getInt();
      byte[] tValue = new byte[size];
      data.get(tValue);
      if (Utils.compare(tKey, key) == 0) {
        result = new byte[tValue.length];
        System.arraycopy(tValue, 0, result, 0, tValue.length);
        break;
      }
    }
    data.clear();

    return result;
  }

  public synchronized long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  public synchronized long getSize() {
    long res = 0;
    if (null != mData) {
      res += mData.DATA.capacity();
    }
    if (null != mIndex) {
      res += mIndex.DATA.capacity();
    }

    return res;
  }

  public synchronized long releaseData() throws IOException {
    long res = 0;

    if (null != mData) {
      res += mData.DATA.capacity();
      mData.close();
    }
    if (null != mIndex) {
      res += mIndex.DATA.capacity();
      mIndex.close();
    }
    return res;
  }
}

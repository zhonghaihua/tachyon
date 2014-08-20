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

  private int[] mLoadedIndex = null;

  public WorkerPartition(TachyonFS tfs, SortedStorePartitionInfo partitionInfo) {
    mTFS = tfs;
    mInfo = new SortedStorePartitionInfo(partitionInfo);
  }

  /**
   * Get the value from a sorted <key/value> pairs. Make it protected to unit test it.
   * 
   * @param key
   *          the key to match
   * @param index
   *          the index
   * @param data
   *          the data buffer containing the sorted <key/value> pairs
   * @return the matched value or byte[0] if there is no match
   */
  protected byte[] getFromSortedData(byte[] key, int[] index, ByteBuffer data) {
    int l = 0;
    int r = index.length;

    byte[] result = new byte[0];
    while (l <= r) {
      int m = (l + r) / 2;
      int pos = index[m];

      data.position(pos);
      int size = data.getInt();
      byte[] tKey = new byte[size];
      data.get(tKey);

      int compare = Utils.compare(tKey, key);
      if (compare == 0) {
        size = data.getInt();
        byte[] tValue = new byte[size];
        data.get(tValue);

        result = new byte[tValue.length];
        System.arraycopy(tValue, 0, result, 0, tValue.length);
        break;
      } else if (compare < 0) {
        l = m + 1;
      } else {
        r = m - 1;
      }
    }

    return result;
  }

  public synchronized byte[] get(byte[] key) throws IOException {
    mLastAccessTimeMs = System.currentTimeMillis();
    if (null == mData) {
      TachyonFile file = mTFS.getFile(mInfo.getDataFileId());
      // TODO add check
      mData = file.readByteBuffer(0);
    }
    if (null == mLoadedIndex) {
      TachyonFile file = mTFS.getFile(mInfo.getIndexFileId());
      // TODO add check
      mIndex = file.readByteBuffer(0);

      ByteBuffer data = mIndex.DATA;
      int size = data.remaining() / 4;
      mLoadedIndex = new int[size];
      for (int k = 0; k < size; k ++) {
        mLoadedIndex[k] = data.getInt();
      }
      mIndex.close();
      mIndex = null;
    }

    return getFromSortedData(key, mLoadedIndex, mData.DATA);
  }

  public synchronized long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   * @return the size of the in memory partition in bytes
   */
  public synchronized long getSize() {
    long res = 0;
    if (null != mData) {
      res += mData.DATA.capacity();
    }
    if (null != mLoadedIndex) {
      res += mLoadedIndex.length * 4;
    }

    return res;
  }

  /**
   * Relased the
   * 
   * @return
   * @throws IOException
   */
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
    if (null != mLoadedIndex) {
      res += mLoadedIndex.length * 4;
      mLoadedIndex = null;
    }
    return res;
  }
}

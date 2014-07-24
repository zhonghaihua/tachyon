package tachyon.r.sorted;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.PartitionSortedStorePartitionInfo;
import tachyon.util.CommonUtils;

/**
 * Each partition contains key/value pairs, and indices.
 * 
 * This class should be abstract, and have different kinds of implementations. But for the first
 * step, it has only one implementation.
 */
public class ClientPartition {
  public static ClientPartition createPartitionSortedStorePartition(TachyonFS tfs, int storeId,
      String storePath, int index) throws IOException {
    return new ClientPartition(tfs, storeId, storePath, index, true);
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final TachyonFS TFS;
  private final int STORE_ID;
  private final String STORE_PATH;
  private final int INDEX;

  // private final Serializer KEY_SER;
  // private final Serializer VALUE_SER;

  private final boolean CREATE;

  private String mPartitionPath;
  private String mDataFilePath;
  private TachyonFile mDataFile;
  private int mDataFileId;

  private OutStream mDataFileOutStream;
  private String mIndexFilePath;
  private TachyonFile mIndexFile;
  private int mIndexFileId;

  private OutStream mIndexFileOutStream;
  private ByteBuffer mStartKey;
  private ByteBuffer mEndKey;

  private int mDataFileLocation;

  ClientPartition(TachyonFS tfs, int storeId, String storePath, int index, boolean create)
      throws IOException {
    TFS = tfs;
    STORE_ID = storeId;
    STORE_PATH = storePath;
    INDEX = index;
    CREATE = create;

    mPartitionPath =
        CommonUtils.concat(STORE_PATH, "partition-" + CommonUtils.addLeadingZero(index, 5));
    mDataFilePath = mPartitionPath + "-data";
    mIndexFilePath = mPartitionPath + "-index";
    LOG.info("Creating KV partition: " + toString());

    if (create) {
      mDataFileId = TFS.createFile(mDataFilePath, Constants.GB);
      mDataFile = TFS.getFile(mDataFileId);
      mDataFileOutStream = mDataFile.getOutStream(WriteType.CACHE_THROUGH);

      mIndexFileId = TFS.createFile(mIndexFilePath, Constants.GB);
      mIndexFile = TFS.getFile(mIndexFileId);
      mIndexFileOutStream = mIndexFile.getOutStream(WriteType.CACHE_THROUGH);

      if (mDataFileId == -1 || mIndexFileId == -1) {
        throw new IOException("Failed to create data file or index file, or both.");
      }
    } else {
      mDataFile = TFS.getFile(mDataFilePath);
      mIndexFile = TFS.getFile(mIndexFilePath);
    }

    mDataFileLocation = 0;
    mStartKey = null;
    mEndKey = null;
  }

  public void close() throws IOException {
    if (CREATE) {
      mDataFileOutStream.close();
      mIndexFileOutStream.close();
      PartitionSortedStorePartitionInfo info = new PartitionSortedStorePartitionInfo();
      info.setStoreId(STORE_ID);
      info.setPartitionIndex(INDEX);
      info.setDataFileId(mDataFileId);
      info.setIndexFileId(mIndexFileId);
      if (mStartKey == null) {
        mStartKey = ByteBuffer.allocate(0);
        mEndKey = ByteBuffer.allocate(0);
      }
      info.setStartKey(mStartKey.array());
      info.setEndKey(mEndKey.array());
      try {
        TFS.r_addPartition(info);
      } catch (Exception e) {
        LOG.error(e);
      }
      LOG.info("closing: " + info);
    }
  }

  public void put(byte[] key, byte[] value) throws IOException {
    if (!CREATE) {
      throw new IOException("Can not put key value pair in non-create mode");
    }
    // put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    if (mStartKey == null) {
      mStartKey = ByteBuffer.allocate(key.length);
      mStartKey.put(key);
      mStartKey.flip();
    }
    mEndKey = ByteBuffer.allocate(key.length);
    mEndKey.put(key);
    mEndKey.flip();
    // mEndKey = ByteBuffer.wrap(value);

    mIndexFileOutStream.write(ByteBuffer.allocate(4).putInt(mDataFileLocation).array());
    mDataFileOutStream.write(ByteBuffer.allocate(4).putInt(key.length).array());
    mDataFileOutStream.write(key);
    mDataFileOutStream.write(ByteBuffer.allocate(4).putInt(value.length).array());
    mDataFileOutStream.write(value);
    mDataFileLocation += 4 + key.length + 4 + value.length;
    // LOG.info("PUT " + CommonUtils.byteArrayToString(key) + " "
    // + CommonUtils.byteArrayToString(value));
  }

  // public void put(ByteBuffer key, ByteBuffer value) {
  // }

  public void put(String key, String value) throws IOException {
    put(key.getBytes(), value.getBytes());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PartitionSortedStorePartition(");
    sb.append("CREATE ").append(CREATE);
    sb.append(" , STORE_PATH ").append(STORE_PATH);
    sb.append(" , mPartitionPath ").append(mPartitionPath);
    sb.append(" , mDataFilePath ").append(mDataFilePath);
    sb.append(" , mIndexFilePath ").append(mIndexFilePath);
    sb.append(")");
    return sb.toString();
  }
}

package tachyon.search;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.ParseException;

import com.google.common.collect.ImmutableList;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.extension.ComponentException;
import tachyon.extension.WorkerComponent;
import tachyon.thrift.SearchStorePartitionInfo;

public class SearchWorker extends WorkerComponent {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final TachyonURI URI;

  // TODO Using TachyonFS is a trick for now.
  private TachyonFS mTFS;

  // Mapping: <storeId, <partitionIndex, Partition>>
  private HashMap<Integer, HashMap<Integer, WorkerShard>> mData;
  private WorkerShard mShard = null;

  public SearchWorker(TachyonURI uri) throws IOException {
    URI = uri;
    LOG.info(URI.toString());
    mTFS = TachyonFS.get(URI);
    mData = new HashMap<Integer, HashMap<Integer, WorkerShard>>();
  }

  public byte[] query(SearchStorePartitionInfo info, byte[] key)
      throws UnsupportedEncodingException, IOException, ParseException {
    // if (!mData.containsKey(info.getStoreId())) {
    // mData.put(info.getStoreId(), new HashMap<Integer, WorkerShard>());
    // }
    //
    // HashMap<Integer, WorkerShard> store = mData.get(info.getStoreId());
    // if (!store.containsKey(info.getShareIndex())) {
    // store.put(info.getShareIndex(), new WorkerShard(mTFS, info));
    // }
    //
    // return store.get(info.getShareIndex()).query(key);
    if (mShard == null) {
      mShard = new WorkerShard(mTFS, null);
    }

    return mShard.query(key);
  }

  @Override
  public List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException {
    minLengthCheck(data);

    WorkerOperationType opType = null;
    try {
      opType = WorkerOperationType.getOpType(data.get(0));
    } catch (IOException e) {
      throw new ComponentException(e);
    }

    try {
      switch (opType) {
      case QUERY: {
        lengthCheck(data, 2, opType.toString());

        // TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        // SearchStorePartitionInfo info = new SearchStorePartitionInfo();
        // deserializer.deserialize(info, data.get(1).array());

        return ImmutableList.of(ByteBuffer.wrap(query(null, data.get(1).array())));
      }
      }
    } catch (UnsupportedEncodingException e) {
      LOG.error(e.getMessage(), e);
      throw new ComponentException(e);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new ComponentException(e);
    } catch (ParseException e) {
      LOG.error(e.getMessage(), e);
      throw new ComponentException(e);
    }

    throw new ComponentException("Unprocessed WorkerOperationType " + opType);
  }

}

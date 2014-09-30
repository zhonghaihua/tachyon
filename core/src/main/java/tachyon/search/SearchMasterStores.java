package tachyon.search;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.collect.ImmutableList;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.extension.ComponentException;
import tachyon.extension.MasterComponent;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonException;

public class SearchMasterStores extends MasterComponent {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private Map<Integer, SearchMasterStore> mStores = new HashMap<Integer, SearchMasterStore>();

  public SearchMasterStores(MasterInfo masterInfo) {
    super(masterInfo);
  }

  public synchronized int createStore(TachyonURI path) throws InvalidPathException,
      FileAlreadyExistException, TachyonException {
    if (!MASTER_INFO.mkdirs(path, true)) {
      return -1;
    }
    int storeId = MASTER_INFO.getFileId(path);
    SearchMasterStore info = new SearchMasterStore(storeId);

    if (mStores.containsKey(info.INODE_ID)) {
      throw new FileAlreadyExistException("The store already exists: " + info);
    }

    mStores.put(info.INODE_ID, info);

    return storeId;
  }

  @Override
  public List<NetAddress> lookup(List<ByteBuffer> data) throws ComponentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException {
    minLengthCheck(data);

    MasterOperationType opType = null;
    try {
      opType = MasterOperationType.getOpType(data.get(0));
    } catch (IOException e) {
      throw new ComponentException(e);
    }

    try {
      switch (opType) {
        case CREATE_STORE: {
          lengthCheck(data, 2, opType.toString());
          int storeId = createStore(new TachyonURI(new String(data.get(1).array())));
          ByteBuffer buf = ByteBuffer.allocate(4);
          buf.putInt(storeId);
          buf.flip();
          return ImmutableList.of(buf);
        }
        case ADD_SHARD: {
          throw new ComponentException("Not supported yet");
        }
        case GET_WORKERS: {
          List<ClientWorkerInfo> workersInfo = MASTER_INFO.getWorkersInfo();
          List<ByteBuffer> result = new ArrayList<ByteBuffer>();
          TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
          for (ClientWorkerInfo workerInfo : workersInfo) {
            byte[] bytes = serializer.serialize(workerInfo.getAddress());
            result.add(ByteBuffer.wrap(bytes));
          }
          return result;
        }
      }
    } catch (InvalidPathException e) {
      throw new ComponentException(e);
    } catch (FileAlreadyExistException e) {
      throw new ComponentException(e);
    } catch (TachyonException e) {
      throw new ComponentException(e);
    } catch (TException e) {
      throw new ComponentException(e);
    }

    throw new ComponentException("Unprocessed MasterOperationType " + opType);
  }

}

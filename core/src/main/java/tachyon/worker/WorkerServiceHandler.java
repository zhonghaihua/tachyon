package tachyon.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.extension.ComponentException;
import tachyon.r.sorted.SortedKVWorkerStore;
import tachyon.search.SearchWorker;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;

/**
 * <code>WorkerServiceHandler</code> handles all the RPC calls to the worker.
 */
public class WorkerServiceHandler implements WorkerService.Iface {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final WorkerStorage mWorkerStorage;

  private SortedKVWorkerStore mSortedKVWorkerStore;
  private SearchWorker mSearchWorker;

  public WorkerServiceHandler(WorkerStorage workerStorage) {
    mWorkerStorage = workerStorage;
    try {
      mSortedKVWorkerStore =
          new SortedKVWorkerStore(new TachyonURI("tachyon://" + mWorkerStorage.getMasterAddress()));
      mSearchWorker =
          new SearchWorker(new TachyonURI("tachyon://" + mWorkerStorage.getMasterAddress()));
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void accessBlock(long blockId) throws TException {
    mWorkerStorage.accessBlock(blockId);
  }

  @Override
  public void addCheckpoint(long userId, int fileId) throws FileDoesNotExistException,
      SuspectedFileSizeException, FailedToCheckpointException, BlockInfoException, TException {
    try {
      mWorkerStorage.addCheckpoint(userId, fileId);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean asyncCheckpoint(int fileId) throws TachyonException, TException {
    try {
      return mWorkerStorage.asyncCheckpoint(fileId);
    } catch (IOException e) {
      throw new TachyonException(e.getMessage());
    }
  }

  @Override
  public void cacheBlock(long userId, long blockId) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, TException {
    try {
      mWorkerStorage.cacheBlock(userId, blockId);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public String getDataFolder() throws TException {
    return mWorkerStorage.getDataFolder();
  }

  @Override
  public String getUserTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserLocalTempFolder(userId);
  }

  @Override
  public String getUserUfsTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserUfsTempFolder(userId);
  }

  @Override
  public void lockBlock(long blockId, long userId) throws TException {
    mWorkerStorage.lockBlock(blockId, userId);
  }

  @Override
  public boolean requestSpace(long userId, long requestBytes) throws TException {
    return mWorkerStorage.requestSpace(userId, requestBytes);
  }

  @Override
  public void returnSpace(long userId, long returnedBytes) throws TException {
    mWorkerStorage.returnSpace(userId, returnedBytes);
  }

  @Override
  public void unlockBlock(long blockId, long userId) throws TException {
    mWorkerStorage.unlockBlock(blockId, userId);
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }

  @Override
  public List<ByteBuffer> x_process(String clz, List<ByteBuffer> data) throws TachyonException,
      TException {
    // TODO make it generic using clz;
    try {
      // return mSortedKVWorkerStore.process(CommonUtils.cloneByteBufferList(data));
      return mSearchWorker.process(CommonUtils.cloneByteBufferList(data));
    } catch (ComponentException e) {
      throw new TachyonException(e.getMessage());
    }
  }
}

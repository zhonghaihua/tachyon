package tachyon.r;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ImmutableList;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.r.sorted.master.MasterOperationType;

public abstract class ClientStoreBase extends StoreBase implements ShardBase {
  // TODO Use TachyonFS for now, create a new handler in the future.
  protected TachyonFS mTachyonFS;

  protected final int ID;

  protected ClientStoreBase(TachyonURI uri, String storeType, boolean create) throws IOException {
    super(uri);

    mTachyonFS = TachyonFS.get(uri.toString());

    if (create) {
      List<ByteBuffer> res =
          mTachyonFS.masterProcess(ImmutableList.of(
              MasterOperationType.CREATE_STORE.toByteBuffer(),
              ByteBuffer.wrap(uri.getPath().getBytes()), ByteBuffer.wrap(storeType.getBytes())));

      ID = res.get(0).getInt();
    } else {
      // TODO use get StoreID;
      ID = mTachyonFS.getFileId(uri.getPath());
    }
  }
}

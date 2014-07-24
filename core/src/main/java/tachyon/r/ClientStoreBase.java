package tachyon.r;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

public abstract class ClientStoreBase extends StoreBase implements ShardBase {
  // TODO Use TachyonFS for now, create a new handler in the future.
  protected TachyonFS mTachyonFS;

  protected final int ID;

  protected ClientStoreBase(TachyonURI uri, String storeType, boolean create)
      throws IOException {
    super(uri);

    mTachyonFS = TachyonFS.get(uri.toString());

    if (create) {
      ID = mTachyonFS.r_createStore(uri.getPath(), storeType);
    } else {
      ID = mTachyonFS.getFileId(uri.getPath());
    }
  }
}

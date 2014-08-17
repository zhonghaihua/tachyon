package tachyon.search;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ImmutableList;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.r.sorted.MasterOperationType;

public class SearchStore {
  private final TachyonURI URI;
  // TODO Use TachyonFS for now, create a new handler in the future.
  private TachyonFS mTachyonFS;
  private final int ID;

  public SearchStore(TachyonURI uri, boolean create) throws IOException {
    URI = uri;
    mTachyonFS = TachyonFS.get(uri.toString());

    if (create) {
      List<ByteBuffer> res =
          mTachyonFS.masterProcess(ImmutableList.of(
              MasterOperationType.CREATE_STORE.toByteBuffer(),
              ByteBuffer.wrap(uri.getPath().getBytes())));

      ID = res.get(0).getInt();
    } else {
      // TODO use get StoreID;
      ID = mTachyonFS.getFileId(uri.getPath());
    }
  }

  public void addDocument(TachyonURI docUri) {
  }

  public String queryAll(String expr) {
    return null;
  }
}

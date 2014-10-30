package tachyon.search;

import org.apache.log4j.Logger;

import tachyon.Constants;

public class SearchMasterStore {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public final int mInodeID;

  public SearchMasterStore(int inodeId) {
    mInodeID = inodeId;
  }
}

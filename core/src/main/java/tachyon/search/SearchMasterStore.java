package tachyon.search;

import org.apache.log4j.Logger;

import tachyon.Constants;

public class SearchMasterStore {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public final int INODE_ID;

  public SearchMasterStore(int inodeId) {
    INODE_ID = inodeId;
  }
}

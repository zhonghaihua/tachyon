package tachyon.extension;

import java.nio.ByteBuffer;
import java.util.List;

import tachyon.master.MasterInfo;
import tachyon.thrift.NetAddress;

/**
 * Extended components running in Tachyon Master
 */
public abstract class MasterComponent extends ComponentUtils {
  protected final MasterInfo MASTER_INFO;

  public MasterComponent(MasterInfo masterInfo) {
    MASTER_INFO = masterInfo;
  }

  public abstract List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException;

  // TODO Move this into process
  public abstract List<NetAddress> lookup(List<ByteBuffer> data) throws ComponentException;
}

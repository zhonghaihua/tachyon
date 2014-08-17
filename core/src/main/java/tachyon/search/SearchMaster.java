package tachyon.search;

import java.nio.ByteBuffer;
import java.util.List;

import tachyon.extension.ComponentException;
import tachyon.extension.MasterComponent;
import tachyon.master.MasterInfo;
import tachyon.thrift.NetAddress;

public class SearchMaster extends MasterComponent {

  public SearchMaster(MasterInfo masterInfo) {
    super(masterInfo);
    // TODO Auto-generated constructor stub
  }

  @Override
  public List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<NetAddress> lookup(List<ByteBuffer> data) throws ComponentException {
    // TODO Auto-generated method stub
    return null;
  }

}

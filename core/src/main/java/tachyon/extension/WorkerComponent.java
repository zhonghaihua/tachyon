package tachyon.extension;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Extended component running in Tachyon Worker.
 */
public abstract class WorkerComponent {
  public abstract List<ByteBuffer> process(List<ByteBuffer> data) throws ComponentException;
}

package tachyon.r.sorted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.thrift.TachyonException;

public class Performance {
  private static TachyonURI STORE_ADDRESS = null;
  private static ClientStore STORE = null;
  private static List<String> KEYS = new ArrayList<String>();

  public static void main(String[] args) throws IOException, TachyonException, TException {
    if (args.length < 1) {
      System.out.println("java -cp core/target/tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar tachyon.r.sorted.Performance <StoreAddress> <keys>");
      System.exit(-1);
    }

    STORE_ADDRESS = new TachyonURI(args[0]);
    // STORE_ADDRESS = new TachyonURI("tachyon://localhost:19998/store_11");
    STORE = ClientStore.getStore(STORE_ADDRESS);

    // STORE.createPartition(0);
    // STORE.put(0, "spark".getBytes(), "5".getBytes());
    // STORE.put(0, "the".getBytes(), "3".getBytes());
    // STORE.closePartition(0);

    System.out.println("There are " + (args.length - 1) + " keys.");
    for (int k = 1; k < args.length; k ++) {
      KEYS.add(args[k]);
      System.out.println(args[k]);
    }
    // KEYS.add("200400");
    // KEYS.add("300410");
    // KEYS.add("500400");
    // KEYS.add("000400");
    // KEYS.add("700400");

    int tests = 10000;
    int have = 0;

    for (int k = 0; k < KEYS.size(); k ++) {
      String key = KEYS.get(k % KEYS.size());
      byte[] result = STORE.get(key.getBytes());
      if (result == null) {
        System.out.println("Key " + key + " does not exist in the store.");
      } else {
        have ++;
        System.out.println("(" + key + "," + new String(result, "UTF-8") + ")");
      }
    }

    long startTimeMs = System.currentTimeMillis();
    for (int k = 0; k < tests; k ++) {
      String key = KEYS.get(k % KEYS.size());
      byte[] result = STORE.get(key.getBytes());
      if (result == null) {
        // System.out.println("Key " + key + " does not exist in the store.");
      } else {
        have ++;
        // System.out.println("(" + key + "," + new String(result, "UTF-8") + ")");
      }
    }
    long endTimeMs = System.currentTimeMillis();

    System.out.println("Total time MS: " + (endTimeMs - startTimeMs) + " " + have);
    System.out.println("Average time MS: " + (endTimeMs - startTimeMs) * 1.0 / tests);
    System.exit(0);
  }
}
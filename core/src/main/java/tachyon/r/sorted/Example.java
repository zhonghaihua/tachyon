package tachyon.r.sorted;

import java.io.IOException;

import tachyon.TachyonURI;

public class Example {
  private static int KEYS = 5;

  public static void createAndFillTheStore(TachyonURI uri) throws IOException {
    ClientStore store = ClientStore.createStore(uri);

    store.createPartition(0);
    for (int k = 0; k < KEYS; k ++) {
      store.put(0, String.valueOf(k).getBytes(), String.valueOf(k * k).getBytes());
    }
    store.closePartition(0);
  }

  public static void getResults(TachyonURI uri) throws IOException {
    ClientStore store = ClientStore.getStore(uri);

    for (int k = 0; k < KEYS * 2; k += 2) {
      byte[] result = store.get(String.valueOf(k).getBytes());
      if (null == result) {
        System.out.println("Key " + k + " does not exist in the store " + uri);
      } else {
        System.out.println("Key " + k + " has value " + new String(result));
      }
    }
  }

  public static void main(String[] args) throws IOException {
    TachyonURI uri = new TachyonURI("");
    createAndFillTheStore(uri);
    getResults(uri);
  }
}

package tachyon.search;

import java.io.IOException;

import tachyon.TachyonURI;

public class Example {
  public static void createAndFillTheStore(TachyonURI uri) throws IOException {
    SearchStore store = new SearchStore(uri, true);

    store.addDocument(new TachyonURI("tachyon://localhost:19998/LICENSE"));
  }

  public static void getResults(TachyonURI uri) throws IOException {
    SearchStore store = new SearchStore(uri, false);

    System.out.println(store.queryAll("main"));
  }

  public static void main(String[] args) throws IOException {
    TachyonURI uri = new TachyonURI("tachyon://localhost:19998/kv3");
    createAndFillTheStore(uri);
    getResults(uri);
  }
}

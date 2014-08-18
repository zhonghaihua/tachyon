package tachyon.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.thrift.SearchStorePartitionInfo;

public class WorkerShard {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private TachyonFS mTFS;
  private SearchStorePartitionInfo mInfo;
  private long mLastAccessTimeMs = System.currentTimeMillis();

  public WorkerShard(TachyonFS tfs, SearchStorePartitionInfo shardInfo) {
    mTFS = tfs;
    // mInfo = new SearchStorePartitionInfo(shardInfo);
  }

  public synchronized byte[] query(byte[] key) throws UnsupportedEncodingException, IOException,
      ParseException {
    return search("/tmp/tmpIndex", "contents", null, new String(key, "UTF-8"), 0, false, 10)
        .getBytes();
  }

  private String search(String index, String field, String queries, String queryString,
      int repeat, boolean raw, int hitsPerPage) throws IOException, ParseException {
    StringBuilder sb = new StringBuilder();

    IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(index)));
    IndexSearcher searcher = new IndexSearcher(reader);
    // :Post-Release-Update-Version.LUCENE_XY:
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);

    BufferedReader in = null;
    if (queries != null) {
      in =
          new BufferedReader(new InputStreamReader(new FileInputStream(queries),
              StandardCharsets.UTF_8));
    } else {
      in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }
    // :Post-Release-Update-Version.LUCENE_XY:
    QueryParser parser = new QueryParser(Version.LUCENE_4_9, field, analyzer);
    while (true) {
      if (queries == null && queryString == null) {                        // prompt the user
        sb.append("Enter query:\n");
      }

      String line = queryString != null ? queryString : in.readLine();

      if (line == null || line.length() == -1) {
        break;
      }

      line = line.trim();
      if (line.length() == 0) {
        break;
      }

      Query query = parser.parse(line);
      sb.append("Searching for: " + query.toString(field)).append("\n");

      if (repeat > 0) {                           // repeat & time as benchmark
        Date start = new Date();
        for (int i = 0; i < repeat; i ++) {
          searcher.search(query, null, 100);
        }
        Date end = new Date();
        sb.append("Time: " + (end.getTime() - start.getTime()) + "ms\n");
      }

      sb.append(doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null
          && queryString == null));

      if (queryString != null) {
        break;
      }
    }
    reader.close();

    return sb.toString();
  }

  /**
   * This demonstrates a typical paging search scenario, where the search engine presents
   * pages of size n to the user. The user can then go to the next page if interested in
   * the next hits.
   * 
   * When the query is executed for the first time, then only enough results are collected
   * to fill 5 result pages. If the user wants to page beyond this limit, then the query
   * is executed another time and all hits are collected.
   */
  private String doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query,
      int hitsPerPage, boolean raw, boolean interactive) throws IOException {
    StringBuilder sb = new StringBuilder();

    // Collect enough docs to show 5 pages
    TopDocs results = searcher.search(query, 5 * hitsPerPage);
    ScoreDoc[] hits = results.scoreDocs;

    int numTotalHits = results.totalHits;
    sb.append(numTotalHits + " total matching documents\n");

    int start = 0;
    int end = Math.min(numTotalHits, hitsPerPage);

    while (true) {
      if (end > hits.length) {
        sb.append("Only results 1 - " + hits.length + " of " + numTotalHits
            + " total matching documents collected.\n");

        hits = searcher.search(query, numTotalHits).scoreDocs;
      }

      end = Math.min(hits.length, start + hitsPerPage);

      for (int i = start; i < end; i ++) {
        if (raw) {                              // output raw format
          sb.append("doc=" + hits[i].doc + " score=" + hits[i].score + "\n");
          continue;
        }

        Document doc = searcher.doc(hits[i].doc);
        String path = doc.get("path");
        if (path != null) {
          sb.append((i + 1) + ". " + path + "\n");
          String title = doc.get("title");
          if (title != null) {
            sb.append("   Title: " + doc.get("title") + "\n");
          }
        } else {
          sb.append((i + 1) + ". " + "No path for this document\n");
        }

      }

      if (!interactive || end == 0) {
        break;
      }

      if (numTotalHits >= end) {
        boolean quit = false;
        while (true) {
          sb.append("Press ");
          if (start - hitsPerPage >= 0) {
            sb.append("(p)revious page, ");
          }
          if (start + hitsPerPage < numTotalHits) {
            sb.append("(n)ext page, ");
          }
          sb.append("(q)uit or enter number to jump to a page.\n");

          String line = in.readLine();
          if (line.length() == 0 || line.charAt(0) == 'q') {
            quit = true;
            break;
          }
          if (line.charAt(0) == 'p') {
            start = Math.max(0, start - hitsPerPage);
            break;
          } else if (line.charAt(0) == 'n') {
            if (start + hitsPerPage < numTotalHits) {
              start += hitsPerPage;
            }
            break;
          } else {
            int page = Integer.parseInt(line);
            if ((page - 1) * hitsPerPage < numTotalHits) {
              start = (page - 1) * hitsPerPage;
              break;
            } else {
              sb.append("No such page\n");
            }
          }
        }
        if (quit)
          break;
        end = Math.min(numTotalHits, start + hitsPerPage);
      }
    }
    return sb.toString();
  }
}

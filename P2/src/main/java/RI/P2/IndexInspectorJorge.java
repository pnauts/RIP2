package RI.P2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/** Simple command-line based search demo. */
public class IndexInspectorJorge {

	private IndexInspectorJorge() {
	}

	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\t IndexInspector [-index dir] [-out file] [-query field string] \n\nSee http://lucene.apache.org/java/4_0/demo.html for details.";
		if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}

		String index = "index";
		String field = "contents";
		String queries = null;
		int repeat = 0;
		boolean raw = false;
		String queryString = null;
		int hitsPerPage = 10;
		Path outdir = null;

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				index = args[i + 1];
				i++;
			} else if ("-query".equals(args[i])) {
				field = args[i + 1];
				queryString = args[i + 2];
				i += 2;
			} else if ("-out".equals(args[i])) {
				outdir = Paths.get(args[i + 1]);
				i++;
			}
		}

		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);

		BufferedReader in = null;
		if (queries != null) {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(queries), "UTF-8"));
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		}
		QueryParser parser = new QueryParser(Version.LUCENE_40, field, analyzer);
		while (true) {
			if (queries == null && queryString == null) { // prompt the user
				System.out.println("Enter query: ");
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
			System.out.println("Searching for: " + query.toString(field));

			if (repeat > 0) { // repeat & time as benchmark
				Date start = new Date();
				for (int i = 0; i < repeat; i++) {
					searcher.search(query, null, 100);
				}
				Date end = new Date();
				System.out.println("Time: " + (end.getTime() - start.getTime()) + "ms");
			}

			doPagingSearch(in, searcher, query,  raw, queries == null && queryString == null, outdir);

			if (queryString != null) {
				break;
			}
		}
		reader.close();
	}

	/**
	 * This demonstrates a typical paging search scenario, where the search
	 * engine presents pages of size n to the user. The user can then go to the
	 * next page if interested in the next hits.
	 * 
	 * When the query is executed for the first time, then only enough results
	 * are collected to fill 5 result pages. If the user wants to page beyond
	 * this limit, then the query is executed another time and all hits are
	 * collected.
	 * 
	 */
	public static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query,boolean raw, boolean interactive, Path outpath) throws IOException {

		// Collect enough docs to show 5 pages
		TotalHitCountCollector collector = new TotalHitCountCollector();
		searcher.search(query,collector);
		TopDocs results = searcher.search(query, Math.max(1,collector.getTotalHits()));
		ScoreDoc[] hits = results.scoreDocs;

		int numTotalHits = results.totalHits;
		System.out.println(numTotalHits + " total matching documents");

		int start = 0;
		int end = numTotalHits;

	
		
		// Crea un writer. Por ahora se crea siempre pero podria crearse
		// solo si se llama con -out
		IndexWriter writer = null;
		if (outpath != null&& outpath.toFile().isDirectory()) {
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer);
			iwc.setOpenMode(OpenMode.CREATE);
			Directory dir = FSDirectory.open(outpath.toFile());
			writer = new IndexWriter(dir, iwc);
		}
		for (int i = start; i < end; i++) {
				if(outpath != null && outpath.toFile().isDirectory()){
					writer.addDocument(searcher.doc(hits[i].doc));				
					
				}
					
				if (raw) { // output raw format
					System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);
					continue;
				}
				
				Document doc = searcher.doc(hits[i].doc);
				String path = doc.get("path");
				if (path != null) {
					System.out.println((i + 1) + ". " + path);
					String title = doc.get("title");
					if (title != null) {
						System.out.println("   Title: " + doc.get("title"));
					}
				} else {
					System.out.println((i + 1) + ". " + "No path for this document");
				}

			}
			if(writer!=null)
				writer.close();
		}
	}

package RI.P2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/** Simple command-line based search demo. */
public class IndexInspector {

	private IndexInspector() {
	}

	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\t IndexInspector [-index dir] [-out file] [-query field string] "
				+ "[-multiquery field1 string1 ... fieldn stringn] [-write file] [-docs i j]\n\n.";
		if (args.length > 0
				&& ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}

		String index = null;
		List<String> fields = new ArrayList<String>();
		List<String> stqueries = new ArrayList<String>();
		List<BooleanClause.Occur> clauses = new ArrayList<BooleanClause.Occur>();
		Path outdir = null;
		Path file = null;
		int minDoc = -1, maxDoc = -1;
		boolean defPrint = true;
		int minFreq = -1, maxFreq = -1;
		String freqFilter = null;
		String term = null;
		int docLimit = -1;

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				index = args[i + 1];
				i++;
			} else if ("-query".equals(args[i])) {
				fields.add(args[i + 1]);
				stqueries.add(args[i + 2]);
				i += 2;
			} else if ("-out".equals(args[i])) {
				outdir = Paths.get(args[i + 1]);
				i++;
			} else if ("-multiquery".equals(args[i])) {
				while (i + 1 < args.length && !args[i + 1].startsWith("-")) {
					fields.add(args[i + 1]);
					i++;
					stqueries.add(args[i + 1]);
					i++;
				}
			} else if ("-write".equals(args[i])) {
				file = Paths.get(args[i + 1]);
			} else if ("-docs".equals(args[i])) {
				minDoc = Integer.parseInt(args[i + 1]);
				maxDoc = Integer.parseInt(args[i + 2]);
				i += 2;
			} else if ("-progquery".equals(args[i])) {
				fields.add(args[i + 1]);
				i++;
				BooleanClause.Occur clause = null;
				while (i + 1 < args.length) {
					if (args[i + 1].startsWith("-")) {
						if (args[i + 1].toUpperCase().equals("-NOT"))
							clause = BooleanClause.Occur.MUST_NOT;
						else if (args[i + 1].toUpperCase().equals("-OR"))
							clause = BooleanClause.Occur.SHOULD;
						else if (args[i + 1].toUpperCase().equals("-AND"))
							clause = BooleanClause.Occur.MUST;
						else
							break;
						i++;
					}
					clauses.add(clause);
					stqueries.add(args[i + 1]);
					i++;
				}

			} else if ("-termswithdocFreq".equals(args[i])) {
				fields.add(args[i + 1]);
				i++;
				minFreq = Integer.parseInt(args[i + 1]);
				i++;
				maxFreq = Integer.parseInt(args[i + 1]);
				i++;
				freqFilter = "df";
			} else if ("-docswithtermFreq".equals(args[i])) {
				fields.add(args[i + 1]);
				i++;
				minFreq = Integer.parseInt(args[i + 1]);
				i++;
				maxFreq = Integer.parseInt(args[i + 1]);
				i++;
				freqFilter = "tf";
			} else if ("-docswithtermFreq2".equals(args[i])) {
				term = args[i + 1];
				i++;
				fields.add(args[i + 1]);
				i++;
				minFreq = Integer.parseInt(args[i + 1]);
				i++;
				maxFreq = Integer.parseInt(args[i + 1]);
				i++;
				freqFilter = "tf2";
			} else if ("-topterms".equals(args[i])) {
				fields.add(args[i + 1]);
				i++;
				docLimit = Integer.parseInt(args[i + 1]);
				i++;
				freqFilter = "tt";
			} else if ("-bottomterms".equals(args[i])) {
				fields.add(args[i + 1]);
				i++;
				docLimit = Integer.parseInt(args[i + 1]);
				i++;
				freqFilter = "bt";
			}
		}

		if (index == null) {
			System.err.println(usage);
			System.exit(-1);
		}

		doPagingSearch(index, stqueries, outdir, fields, clauses, file, minDoc,
				maxDoc, defPrint, minFreq, maxFreq, freqFilter, term, docLimit);
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
	public static void doPagingSearch(String index, List<String> queries,
			Path outpath, List<String> fields,
			List<BooleanClause.Occur> clauses, Path file, int minDoc,
			int maxDoc, boolean defPrint, int minFreq, int maxFreq,
			String freqFilter, String term, int docLimit) throws IOException {

		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(
				index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);

		String[] auxfields = new String[fields.size()];
		int i = 0;
		for (String field : fields) {
			auxfields[i] = field;
			i++;
		}

		MultiFieldQueryParser parser = new MultiFieldQueryParser(
				Version.LUCENE_40, auxfields, analyzer);

		// Collect enough docs to show 5 pages
		if (freqFilter != null) {

			AtomicReader atomicReader = SlowCompositeReaderWrapper
					.wrap((CompositeReader) reader);
			Fields allfields = atomicReader.fields();
			Terms terms = allfields.terms(fields.get(0));
			TermsEnum termsEnum = terms.iterator(null);
			String nombre = termsEnum.term().utf8ToString();
			Map<Float, String> scoreMap = new HashMap<Float, String>();
			int numDocs = reader.numDocs();
			DefaultSimilarity dsm = new DefaultSimilarity();

			while (termsEnum.next() != null) {
				nombre = termsEnum.term().utf8ToString();
				long docFreq = termsEnum.docFreq();

				if (freqFilter.equals("df") && docFreq >= minFreq
						&& docFreq <= maxFreq) {
					System.out.println("Term: " + nombre + " DF: " + docFreq);
					continue;
				}

				if (freqFilter.equals("tt") || freqFilter.equals("bt")) {
					scoreMap.put(
							dsm.tf(termsEnum.totalTermFreq())
									* dsm.idf(docFreq, numDocs), nombre);
				}

				else {
					DocsEnum docsEnum = termsEnum.docs(null, null);
					while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
						int id = docsEnum.docID();
						long termFreq = docsEnum.freq();
						if (freqFilter.equals("tf")) {
							if (termFreq >= minFreq && termFreq <= maxFreq)
								System.out.println("DocId: "
										+ Integer.toString(id) + " Term: "
										+ nombre + " TF: " + termFreq);
						} else if (freqFilter.equals("tf2")
								&& nombre.equals(term) && termFreq >= minFreq
								&& termFreq <= maxFreq) {
							System.out.println("Term: " + nombre + " appears "
									+ termFreq + " times at Doc: " + id);
						}
					}
				}
			}
			if (!scoreMap.isEmpty()) {
				mapSort(scoreMap, docLimit, freqFilter);
				return;
			}

			atomicReader.close();
			return;
		}

		TotalHitCountCollector collector = new TotalHitCountCollector();

		BooleanQuery booleanQuery = new BooleanQuery();
		if (fields.size() == 0) {
			Query query = new MatchAllDocsQuery();
			booleanQuery.add(query, BooleanClause.Occur.SHOULD);
		} else if (clauses == null) {
			for (i = 0; i < fields.size(); i++) {
				Query query = new TermQuery(new Term(fields.get(i),
						queries.get(i)));
				booleanQuery.add(query, clauses.get(i));
			}
		} else {
			String field = fields.get(0);
			for (i = 0; i < queries.size(); i++) {
				Query query = null;
				if (queries.get(i).equals("*:*"))
					query = new MatchAllDocsQuery();
				else
					query = new TermQuery(new Term(field, queries.get(i)));
				booleanQuery.add(query, clauses.get(i));
			}
		}

		searcher.search(booleanQuery, collector);

		TopDocs results;
		if (maxDoc == -1)
			results = searcher.search(booleanQuery,
					Math.max(1, collector.getTotalHits()));
		else
			results = searcher.search(booleanQuery, Math.max(1, maxDoc + 1));

		ScoreDoc[] hits = results.scoreDocs;

		int start = 0;
		int end = 0;
		if (maxDoc != -1) {
			start = minDoc;
			end = maxDoc + 1;
		} else {
			int numTotalHits = results.totalHits;
			System.out.println(numTotalHits + " total matching documents");
			end = numTotalHits;
		}

		// Crea un writer. Por ahora se crea siempre pero podria crearse
		// solo si se llama con -out
		IndexWriter writer = null;
		if (outpath != null && outpath.toFile().isDirectory()) {
			Analyzer wanalyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
			IndexWriterConfig iwc = new IndexWriterConfig(
					Version.LUCENE_4_10_0, wanalyzer);
			iwc.setOpenMode(OpenMode.CREATE);
			Directory dir = FSDirectory.open(outpath.toFile());
			writer = new IndexWriter(dir, iwc);
		}

		List<String> lines = null;
		if (file != null && file.toFile().isFile())
			lines = new ArrayList<String>();

		for (i = start; i < end; i++) {
			Document doc = searcher.doc(hits[i].doc);

			if (outpath != null && outpath.toFile().isDirectory()) {
				writer.addDocument(doc);
			}

			if (lines != null)
				lines.add(doc.toString());

			if (defPrint) {
				System.out.println("DocID: " + i);
				System.out.println("Title: " + doc.get("title"));
				System.out.println("Score: " + hits[i].score);
				if (maxDoc != -1)
					System.out.println("Content:\n" + doc.toString());
			}
			System.out.println();
		}

		if (lines != null)
			Files.write(file, lines, Charset.forName("UTF-8"),
					StandardOpenOption.CREATE);

		if (writer != null)
			writer.close();
		reader.close();
	}

	private static void mapSort(Map<Float, String> unsortMap, int docLimit,
			String freqFilter) {

		Map<Float, String> map = null;

		if ("tt".equals(freqFilter))
			map = new TreeMap<Float, String>(unsortMap).descendingMap();

		else if ("bt".equals(freqFilter))
			map = new TreeMap<Float, String>(unsortMap);

		for (Map.Entry<Float, String> entry : map.entrySet()) {
			if (docLimit == 0)
				return;
			System.out.println(entry.getValue() + " " + entry.getKey());
			docLimit--;
		}
	}
}

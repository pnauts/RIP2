package RI.P2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SingleTermsEnum;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Fields ;
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
import org.apache.lucene.search.TermRangeTermsEnum;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

/** Simple command-line based search demo. */
public class IndexInspectorJorge {

	private IndexInspectorJorge() {
	}

	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\t IndexInspector [-index dir] [-out file] [-query field string] \n\nSee http://lucene.apache.org/java/4_0/demo.html for details.";
		if (args.length > 0
				&& ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}

		String index = "index";
		List<String> fields = new ArrayList<String>();
		List<String> stqueries = new ArrayList<String>();
		Path outdir = null;
		int minindex=0 , maxindex=0;
		boolean termsdf=false,docstf=false,docstf2=false;
		Term term = null;

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
				while (i+1 < args.length && !args[i + 1].startsWith("-")) {
					fields.add(args[i + 1]);
					i++;
					stqueries.add(args[i + 1]);
					i ++;
				}
			}else if ("-termswithdocFreq".equals(args[i])) {
				fields.add(args[i+1]);
				minindex=Integer.parseInt(args[i+2]);
				maxindex=Integer.parseInt(args[i+3]);
				termsdf=true;
				i+=3;
			}else if ("-docswithtermFreq".equals(args[i])) {
				fields.add(args[i+1]);
				minindex=Integer.parseInt(args[i+2]);
				maxindex=Integer.parseInt(args[i+3]);
				docstf=true;
				i+=3;
			}else if ("-docswithtermFreq2".equals(args[i])) {
				String field = args[i+2];
				term=new Term(field,args[i+1]);
				minindex=Integer.parseInt(args[i+3]);
				maxindex=Integer.parseInt(args[i+4]);
				docstf2=true;
				fields.add(field);
				i+=4;
				
			}
		}
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(
				index)));
		AtomicReader atomicReader = SlowCompositeReaderWrapper.wrap((CompositeReader) reader);
		IndexSearcher searcher = new IndexSearcher(atomicReader);
		
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
		
		String [] auxfields = new String[fields.size()];
		int i = 0;
		for (String field:fields){
			auxfields[i] = field;
			i++;
		}
		
		MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_40, auxfields, analyzer);
		
			doPagingSearch(searcher, stqueries, outdir, fields,atomicReader,term,termsdf,docstf,docstf2,minindex,maxindex);
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
	public static void doPagingSearch(
			IndexSearcher searcher, List<String> queries, 
			Path outpath, List<String> fields,AtomicReader reader,Term term,boolean termdf,boolean docstf,boolean docstf2,int minindex,int maxindex) throws IOException {

		// Collect enough docs to show 5 pages
		TotalHitCountCollector collector = new TotalHitCountCollector();
		
		Query booleanQuery = new BooleanQuery();
		if(queries != null && queries.size()>0){
			for(int i = 0; i < fields.size(); i++){
				Query query = new TermQuery(new Term(fields.get(i), queries.get(i)));
				((BooleanQuery) booleanQuery).add(query, BooleanClause.Occur.SHOULD);
			}
		}else{
			booleanQuery=new MatchAllDocsQuery();
		}
				
		searcher.search(booleanQuery, collector);
		TopDocs results = searcher.search(booleanQuery,
				Math.max(1, collector.getTotalHits()));
		ScoreDoc[] hits = results.scoreDocs;

	
		

		int numTotalHits = results.totalHits;
		System.out.println(numTotalHits + " total matching documents");

		int start = 0;
		int end = numTotalHits;

		// Crea un writer. Por ahora se crea siempre pero podria crearse
		// solo si se llama con -out
		IndexWriter writer = null;
		if (outpath != null && outpath.toFile().isDirectory()) {
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
			IndexWriterConfig iwc = new IndexWriterConfig(
					Version.LUCENE_4_10_0, analyzer);
			iwc.setOpenMode(OpenMode.CREATE);
			Directory dir = FSDirectory.open(outpath.toFile());
			writer = new IndexWriter(dir, iwc);
		}
		Fields allfields=null;
		allfields=reader.fields();
		Terms terms =null;
		TermsEnum termsEnum = null;
		if(termdf || docstf || docstf2){
			
			
			Map<String,Long> tf = new HashMap();
			Map<String,Long> df = new HashMap();
			
			
			for(String field: allfields){
				System.out.println("Field: "+ field);
				terms=allfields.terms(field);
				termsEnum=terms.iterator(null);
				while(termsEnum.next()!=null){
					String nombre=termsEnum.term().utf8ToString();
					long totalTermFreq=termsEnum.totalTermFreq();
					//long docFreq=termsEnum.docFreq();
					if(termdf&&totalTermFreq>=minindex&&totalTermFreq<maxindex){
						System.out.println(nombre+ "    " + totalTermFreq);						
					}	
					DocsEnum docsEnum=termsEnum.docs(null, null);
					int docf=0;
					while(docsEnum.nextDoc()!=DocIdSetIterator.NO_MORE_DOCS){
						int id=docsEnum.docID();
						long termFreq=docsEnum.freq();
						if(docstf&&termFreq>=minindex&&termFreq<maxindex){
							docf++;
						}
						//System.out.println(termsEnum.term().utf8ToString());
						//System.out.println(term.text());
						if(docstf2&&field.equals(fields.get(0))&&term.text().equals(termsEnum.term().utf8ToString())&&termFreq>=minindex&&termFreq<maxindex){
							System.out.println("docid : " +Integer.toString(id)+ "  termfreq  " + termFreq);							
						}
					}
					if(docf>0)
						System.out.println(nombre + "\t\t\t" + docf);
					
				}
			}	
		}	
		
		for (int i = start; i < end; i++) {
			if (outpath != null && outpath.toFile().isDirectory()) {
				writer.addDocument(searcher.doc(hits[i].doc));

			}
			
				
			
			
			
			//  t o t a
			//System.out.println(terminos.toString());
			Document doc = searcher.doc(hits[i].doc);
			String path = doc.get("path");
			

		}
		if (writer != null)
			writer.close();
	}
}

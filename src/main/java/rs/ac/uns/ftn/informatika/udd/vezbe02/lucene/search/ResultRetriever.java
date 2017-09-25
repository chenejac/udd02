package rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.search;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.DocumentHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.PDFHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.TextDocHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.Word2007Handler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.WordHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.RequiredHighlight;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.ResultData;


@SuppressWarnings({ "resource", "unchecked" })
public class ResultRetriever {
	
	private static int maxHits = 10;
	
	private static TransportClient client;
	
	static {
		Settings settings = Settings.builder()
		        .put("cluster.name", "uns").build();
		client = new PreBuiltTransportClient(settings);
		try {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	public static void setMaxHits(int maxHits) {
		ResultRetriever.maxHits = maxHits;
	}

	public static int getMaxHits() {
		return ResultRetriever.maxHits;
	}

	public static List<ResultData> getResults(org.elasticsearch.index.query.QueryBuilder query,
			List<RequiredHighlight> requiredHighlights) {
		if (query == null) {
			return null;
		}
			
		List<ResultData> results = new ArrayList<ResultData>();

		
		SearchResponse response = client.prepareSearch("digitallibrary")
		        .setTypes("book")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(query)
		        .setSize(maxHits)
		        .get();
		
		
		SearchHits hits = response.getHits();

		ResultData rd;
		
		for (SearchHit sd : hits.getHits()) {
			List<Object> allKeywords = sd.getField("keyword").getValues();
			String keywords = "";
			for (Object keyword : allKeywords) {
				keywords += keyword.toString().trim() + " ";
			}
			keywords = keywords.trim();
			String title =sd.getField("title").getValue();
			String location = sd.getField("filename").getValue();
			String highlight = "";
			for (HighlightField hf : sd.getHighlightFields().values()) {
				for (RequiredHighlight rh : requiredHighlights) {
					if(hf.getName().equals(rh.getFieldName())){
						highlight += hf.fragments().toString();
					}
				}
			}
			rd = new ResultData(title, keywords, location,
					highlight);
			results.add(rd);
		}
		return results;
	}
	
	protected static DocumentHandler getHandler(String fileName){
		if(fileName.endsWith(".txt")){
			return new TextDocHandler();
		}else if(fileName.endsWith(".pdf")){
			return new PDFHandler();
		}else if(fileName.endsWith(".doc")){
			return new WordHandler();
		}else if(fileName.endsWith(".docx")){
			return new Word2007Handler();
		}else{
			return null;
		}
	}
}

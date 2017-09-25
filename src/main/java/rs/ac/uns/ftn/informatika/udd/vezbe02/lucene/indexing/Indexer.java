package rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.DocumentHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.PDFHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.TextDocHandler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.Word2007Handler;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.indexing.handlers.WordHandler;


public class Indexer {
	

	private TransportClient client;
	
	private static Indexer indexer = new Indexer();
	
	public static Indexer getInstance(){
		return indexer;
	}
	
	@SuppressWarnings("unchecked")
	private Indexer(String address, int port) {
		try {
			Settings settings = Settings.builder()
			        .put("cluster.name", "uns").build();
			client = new PreBuiltTransportClient(settings);
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), port));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	private Indexer() {
		this("localhost", 9300);
	}
	
	
	/**
	 * Od dobijenih vrednosti se konstruise Term po kojem se vrsi pretraga dokumenata
	 * Dokumenti koji zadovoljavaju uslove pretrage ce biti obrisani
	 * 
	 * @param fieldName naziv polja
	 * @param value vrednost polja
	 * @return
	 */
	public boolean delete(String filename){
		BulkByScrollResponse response =
			    DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
			        .filter(QueryBuilders.matchQuery("filename", filename)) 
			        .filter(QueryBuilders.matchQuery("type.value", "book")) 
			        .source("digitallibrary")                                  
			        .get();                                             

		long deleted = response.getDeleted();
		if(deleted != 0)
			return true;
		else
			return false;
	}
	
	public boolean add(XContentBuilder contentBuilder){
		IndexResponse response = client.prepareIndex("digitallibrary", "book")
		        .setSource(contentBuilder)
		        .get();
		if(response.getResult().equals(Result.CREATED))
			return true;
		else if (response.getVersion() > 0)
				return true;
			else
				return false;
	}
	
	public boolean updateDocument(String id, XContentBuilder contentBuilder) throws InterruptedException, ExecutionException{		
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("index");
		updateRequest.type("type");
		updateRequest.id(id);
		updateRequest.doc(contentBuilder);
		UpdateResponse response = client.update(updateRequest).get();
		if(response.getResult().equals(Result.UPDATED))
			return true;
		else if (response.getVersion() > 0)
				return true;
			else
				return false;
	}
	
	/**
	 * 
	 * @param file Direktorijum u kojem se nalaze dokumenti koje treba indeksirati
	 */
	public int index(File file){		
		DocumentHandler handler = null;
		String fileName = null;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		int retVal = 0;
		try {
			File[] files;
			if(file.isDirectory()){
				files = file.listFiles();
			}else{
				files = new File[1];
				files[0] = file;
			}
			for(File newFile : files){
				if(newFile.isFile()){
					fileName = newFile.getName();
					handler = getHandler(fileName);
					if(handler == null){
						System.out.println("Nije moguce indeksirati dokument sa nazivom: " + fileName);
						continue;
					}
					bulkRequest.add(client.prepareIndex("digitallibrary", "book")
					        .setSource(handler.getIndexUnit(newFile).getXContentBuilder()		                  )
					        );
				} else if (newFile.isDirectory()){
					retVal += index(newFile);
				}
			}
			bulkResponse = bulkRequest.get();
			System.out.println("indexing done");
		} catch (IOException e) {
			System.out.println("indexing NOT done");
		}
		if(bulkResponse == null)
			return -1;
		else{ 
			retVal += bulkResponse.getItems().length;
			return retVal;
		}
	}
	
	protected void finalize() throws Throwable {
		this.client.close();
	}
	
	public DocumentHandler getHandler(String fileName){
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
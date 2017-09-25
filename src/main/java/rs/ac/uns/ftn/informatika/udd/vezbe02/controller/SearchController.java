package rs.ac.uns.ftn.informatika.udd.vezbe02.controller;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.AdvancedQuery;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.RequiredHighlight;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.ResultData;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.SearchType;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.model.SimpleQuery;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.search.QueryBuilder;
import rs.ac.uns.ftn.informatika.udd.vezbe02.lucene.search.ResultRetriever;

@RestController
public class SearchController {

		@PostMapping(value="/search/term", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchTermQuery(@RequestBody SimpleQuery simpleQuery) throws Exception {		
			org.elasticsearch.index.query.QueryBuilder query= QueryBuilder.buildQuery(SearchType.regular, simpleQuery.getField(), simpleQuery.getValue());
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(simpleQuery.getField(), simpleQuery.getValue()));
			List<ResultData> results = ResultRetriever.getResults(query, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/fuzzy", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchFuzzy(@RequestBody SimpleQuery simpleQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query= QueryBuilder.buildQuery(SearchType.fuzzy, simpleQuery.getField(), simpleQuery.getValue());
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(simpleQuery.getField(), simpleQuery.getValue()));
			List<ResultData> results = ResultRetriever.getResults(query, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/prefix", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchPrefix(@RequestBody SimpleQuery simpleQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query= QueryBuilder.buildQuery(SearchType.prefix, simpleQuery.getField(), simpleQuery.getValue());
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(simpleQuery.getField(), simpleQuery.getValue()));
			List<ResultData> results = ResultRetriever.getResults(query, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/range", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchRange(@RequestBody SimpleQuery simpleQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query= QueryBuilder.buildQuery(SearchType.range, simpleQuery.getField(), simpleQuery.getValue());
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(simpleQuery.getField(), simpleQuery.getValue()));
			List<ResultData> results = ResultRetriever.getResults(query, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/phrase", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchPhrase(@RequestBody SimpleQuery simpleQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query= QueryBuilder.buildQuery(SearchType.phrase, simpleQuery.getField(), simpleQuery.getValue());
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(simpleQuery.getField(), simpleQuery.getValue()));
			List<ResultData> results = ResultRetriever.getResults(query, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/boolean", consumes="application/json")
		public ResponseEntity<List<ResultData>> searchBoolean(@RequestBody AdvancedQuery advancedQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query1 = QueryBuilder.buildQuery(SearchType.regular, advancedQuery.getField1(), advancedQuery.getValue1());
			org.elasticsearch.index.query.QueryBuilder query2 = QueryBuilder.buildQuery(SearchType.regular, advancedQuery.getField2(), advancedQuery.getValue2());
			
			BoolQueryBuilder builder = QueryBuilders.boolQuery();
			if(advancedQuery.getOperation().equalsIgnoreCase("AND")){
				builder.must(query1);
				builder.must(query2);
			}else if(advancedQuery.getOperation().equalsIgnoreCase("OR")){
				builder.should(query1);
				builder.should(query2);
			}else if(advancedQuery.getOperation().equalsIgnoreCase("NOT")){
				builder.must(query1);
				builder.mustNot(query2);
			}
			
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			rh.add(new RequiredHighlight(advancedQuery.getField1(), advancedQuery.getValue1()));
			rh.add(new RequiredHighlight(advancedQuery.getField2(), advancedQuery.getValue2()));
			List<ResultData> results = ResultRetriever.getResults(builder, rh);			
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
		
		@PostMapping(value="/search/queryParser", consumes="application/json")
		public ResponseEntity<List<ResultData>> search(@RequestBody SimpleQuery simpleQuery) throws Exception {
			org.elasticsearch.index.query.QueryBuilder query=QueryBuilders.queryStringQuery(simpleQuery.getValue());			
			List<RequiredHighlight> rh = new ArrayList<RequiredHighlight>();
			List<ResultData> results = ResultRetriever.getResults(query, rh);
			return new ResponseEntity<List<ResultData>>(results, HttpStatus.OK);
		}
	
		
	
}

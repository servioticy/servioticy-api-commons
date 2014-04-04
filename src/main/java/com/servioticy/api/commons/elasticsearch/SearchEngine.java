package com.servioticy.api.commons.elasticsearch;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.sort.SortOrder;

public class SearchEngine {
//Query by prefix:
	//{"query":{"bool":{"must":[{"prefix":{"couchbaseDocument.meta.id":"139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8"}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
//AND lastUpdate range
	//{"query":{"bool":{"must":[{"prefix":{"couchbaseDocument.meta.id":"139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8"}},{"range":{"couchbaseDocument.doc.lastUpdate":{"from":"1395946785","to":"1395946786"}}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
	
	
	static TransportClient client = null; 
	static {
		
		Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", "serviolastic").build();		
		
		client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("192.168.56.101", 9300));	
	
	}
	
	
	public static List<String> searchUpdates(String stream, String SOid, SearchCriteria filter) {
				
		
		SearchResponse response = client.prepareSearch("elastic").setTypes("couchbaseDocument")
		.setQuery(QueryBuilders.matchPhrasePrefixQuery("couchbaseDocument.meta.id",SOid+"-"+stream+"-"))
		.setPostFilter(filter.buildFilter())
        .execute().actionGet();
		
		List<String> res = new ArrayList<String>();
		
		if(response != null) {
			SearchHits hits = response.getHits();
			if(hits != null) {
				long count = hits.getTotalHits();
				if(count > 0) {				
					Iterator<SearchHit> iter = hits.iterator();
					while(iter.hasNext()) {
						SearchHit hit = iter.next();
						res.add(hit.getId());						
					}
				}
			}
		}
		
		return res; 		
	}
	
	
	public static String getGropLastUpdateDocId(String stream, List<String> SOids) {
	
		
		OrFilterBuilder IdsFilter = FilterBuilders.orFilter();
		for(String id : SOids)
			IdsFilter.add(FilterBuilders.prefixFilter("couchbaseDocument.meta.id", id));
		
		
		SearchResponse response = client.prepareSearch("elastic").setTypes("couchbaseDocument")
		.setFrom(0).setSize(1)
        .setQuery(QueryBuilders.regexpQuery("couchbaseDocument.meta.id",".*-"+stream+"-.*"))
        .setPostFilter(IdsFilter)
        .addSort("couchbaseDocument.doc.lastUpdate", SortOrder.DESC)
        .execute().actionGet();
		
		if(response.getHits().getTotalHits() > 0)
			return response.getHits().getHits()[0].getId();		
		else		
			return null;
		
	}
	
	
	
	public static long getLastUpdate(String SOid, String stream) {
		//https://github.com/elasticsearch/elasticsearch/blob/master/src/test/java/org/elasticsearch/search/aggregations/metrics/MaxTests.java		

		
		SearchResponse response = client.prepareSearch("elastic").setTypes("couchbaseDocument")
		.setQuery(QueryBuilders.matchPhrasePrefixQuery("couchbaseDocument.meta.id",SOid+"-"+stream+"-"))
        .addAggregation(max("max").field("lastUpdate"))
        .execute().actionGet();

		Max max = response.getAggregations().get("max");
		
		return (long)max.getValue(); 
	}
	
	public static List<String> getAllUpdatesId(String SOid, String stream) {		
		
		SearchResponse response = client.prepareSearch("elastic").setTypes("couchbaseDocument")
		.setQuery(QueryBuilders.matchPhrasePrefixQuery("couchbaseDocument.meta.id",SOid+"-"+stream+"-"))
        .execute().actionGet();
		
		List<String> res = new ArrayList<String>();
		
		if(response != null) {
			SearchHits hits = response.getHits();
			if(hits != null) {
				long count = hits.getTotalHits();
				if(count > 0) {				
					Iterator<SearchHit> iter = hits.iterator();
					while(iter.hasNext()) {
						SearchHit hit = iter.next();
						res.add(hit.getId());						
					}
				}
			}
		}
		
		return res; 
	}
	
	
}

package com.servioticy.api.commons.elasticsearch;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.sort.SortOrder;

import com.servioticy.api.commons.utils.Config;

public class SearchEngine {
//Query by prefix:
    //{"query":{"bool":{"must":[{"prefix":{"couchbaseDocument.meta.id":"139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8"}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
//AND lastUpdate range
    //{"query":{"bool":{"must":[{"prefix":{"couchbaseDocument.meta.id":"139594599486709ceb6bfdddb48cfabfcce0e6a9cf6c8"}},{"range":{"couchbaseDocument.doc.lastUpdate":{"from":"1395946785","to":"1395946786"}}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}

//    private static TransportClient client = null;
    private static Client client = Config.elastic_client;
    private static String soupdates = Config.soupdates;
    private static String subscriptions = Config.subscriptions;
    static {

//        Settings settings = ImmutableSettings.settingsBuilder()
//        									 .put("cluster.name", Config.elastic_cluster).build();
//
//        client = new TransportClient(settings)
//        	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
////        	.addTransportAddress(new InetSocketTransportAddress("192.168.56.101", 9300));

        // Testing
//        Node node = nodeBuilder().clusterName(Config.elastic_cluster).client(true).node();
//        client = node.client();
    }

    public static List<String> searchUpdates(String soId, String streamId, SearchCriteria filter) {

        SearchResponse scan  = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
        		.setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId.toLowerCase() + "-"))
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        System.out.println("QUERY: "+client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId.toLowerCase() + "-"))
                .setPostFilter(filter.buildFilter()).toString());

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId.toLowerCase() + "-"))
                .setSize((int)scan.getHits().getTotalHits())
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

    public static String getGroupLastUpdateDocId(String streamId, List<String> soIds) {

        OrFilterBuilder IdsFilter = FilterBuilders.orFilter();
        for(String id : soIds)
            IdsFilter.add(FilterBuilders.prefixFilter("meta.id", id));

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setFrom(0).setSize(1)
                .setQuery(QueryBuilders.regexpQuery("meta.id",".*-" + streamId.toLowerCase() + "-.*"))
                .setPostFilter(IdsFilter)
                .addSort("doc.lastUpdate", SortOrder.DESC)
                .execute().actionGet();

        if(response.getHits().getTotalHits() > 0)
            return response.getHits().getHits()[0].getId();
        else
            return null;

    }

    public static long getLastUpdateTimeStamp(String soId, String streamId) {
        //https://github.com/elasticsearch/elasticsearch/blob/master/src/test/java/org/elasticsearch/search/aggregations/metrics/MaxTests.java

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .addAggregation(max("max").field("lastUpdate"))
                .execute().actionGet();

        Max max = response.getAggregations().get("max");

        return (long)max.getValue();
    }

    public static List<String> getAllUpdatesId(String soId, String streamId) {

        SearchResponse scan = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setSize((int)scan.getHits().getTotalHits())
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

    public static List<String> getExternalSubscriptionsByStream(String soId, String streamId) {

        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setSearchType(SearchType.SCAN)
                .setPostFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("doc.callback", "internal")))
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setPostFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("doc.callback", "internal")))
                .setSize((int)scan.getHits().getTotalHits())
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

    public static List<String> getAllSubscriptionsByStream(String soId, String streamId) {

        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId.toLowerCase() + "-"))
                .setSize((int)scan.getHits().getTotalHits())
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
    
    public static List<String> getAllSubscriptionsBySrcAndDst(String soId) {

        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                		  .should(QueryBuilders.matchQuery("doc.source", soId))
                		  .should(QueryBuilders.matchQuery("doc.destination", soId))
                		 )
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                		  .should(QueryBuilders.matchQuery("doc.source", soId))
                		  .should(QueryBuilders.matchQuery("doc.destination", soId))
                		 )
                .setSize((int)scan.getHits().getTotalHits())
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

    public static String getSusbcriptionDocId(String subId) {

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.matchQuery("meta.id", subId))
                .execute().actionGet();

        if(response.getHits().getTotalHits() > 0)
            return response.getHits().getHits()[0].getId();
        else
            return null;
    }


}



package com.servioticy.api.commons.elasticsearch;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.ws.rs.core.Response;

import org.elasticsearch.action.count.CountResponse;
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
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
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
        		.setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId + "-"))
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        System.out.println("QUERY: "+client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId + "-"))
                .setPostFilter(filter.buildFilter()).toString());

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id",soId+"-" + streamId + "-"))
                .setSize((int)scan.getHits().getTotalHits())
                .setPostFilter(filter.buildFilter())
                .execute().actionGet();
        
        System.out.println("response = " + response);

        List<String> res = new ArrayList<String>();

        if(response != null) {
            SearchHits hits = response.getHits();
            System.out.println("hits = " + hits);
            if(hits != null) {
                long count = hits.getTotalHits();
                System.out.println("count = " + count);
                if(count > 0) {
                    Iterator<SearchHit> iter = hits.iterator();
                    while(iter.hasNext()) {
                        SearchHit hit = iter.next();
                        System.out.println("hit = " + hit.getId());
                        res.add(hit.getId());
                    }
                }
            }
        }
        
        System.out.println("res = " + res);

        return res;
    }

    public static String getAllUpdatesLastMinute() {
        
      CouchbaseClient cli_reputation = Config.cli_reputation;
        
      DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
      formatter.setTimeZone(TimeZone.getTimeZone("GMT"));;

//      String startKey = formatter.format(new Date(1445617834345L));
//      String startKey = formatter.format(new Date(1445617894000L));
//      String startKey = formatter.format(new Date(1445617834345L));
//      String endKey = formatter.format(new Date(1445617894000L));

      String endKey = formatter.format(System.currentTimeMillis());
      long millis = System.currentTimeMillis() - 60000;
      String startKey = formatter.format(millis);

      System.out.println("startKey = " + startKey);
      System.out.println("endKey = " + endKey);

      Map<String, Integer> allUpdates = new HashMap<String, Integer>();
      JsonNode root;

      try {
        View view = cli_reputation.getView("webobject", "byDate");

        Query query = new Query();
        query.setStale(Stale.FALSE)
             .setRangeStart(startKey)
             .setRangeEnd(endKey)
             .setInclusiveEnd(true);
//             .setRangeStart(ComplexKey.of(startKey, "1437553443075a50e92918f114528ad264844d1c8b34c"));
        
        ViewResponse result = cli_reputation.query(view, query);
        Integer value;
        for(ViewRow row : result) {
          if (row.getKey() != null) {
              System.out.println(row.getValue());
              value = allUpdates.get(row.getValue());
              if (value == null)
                  value = 0;
              value++;
              allUpdates.put(row.getValue(), value);
          }
        }
      } catch (Exception e){
        throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Accessing the view: "+e.getMessage());
      }

      ObjectMapper mapper = new ObjectMapper();
      ArrayNode array = mapper.createArrayNode();

      for (Map.Entry<String, Integer> entry : allUpdates.entrySet()) {
          root = mapper.createObjectNode();
          ((ObjectNode) root).put("doc_count", entry.getValue());
          ((ObjectNode) root).put("key", entry.getKey());
          array.add(root);
      }
      
      root = mapper.createObjectNode();
      ((ObjectNode) root).put("message", array);

//        String mapAsJson = new ObjectMapper().writeValueAsString(allUpdates);
//        System.out.println(mapAsJson);


    return root.toString();
        
//        long millis = System.currentTimeMillis() - 60000;
//
//        System.out.println("QUERY: "+client.prepareSearch("zseclivewo").setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.boolQuery()
//                                       .must(QueryBuilders.matchQuery("doc.src.webobject", "true"))
//                                       .must(QueryBuilders.rangeQuery("doc.date").from(millis))
//                                       )
//                .setSize(0)
//                .addAggregation(terms("distinct_soids").field("soid"))
//                );
//
//        SearchResponse response = client.prepareSearch("zseclivewo").setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.boolQuery()
//                                       .must(QueryBuilders.matchQuery("doc.src.webobject", "true"))
//                                       .must(QueryBuilders.rangeQuery("doc.date").from(millis))
//                                       )
//                .setSize(0)
//                .addAggregation(terms("distinct_soids").field("soid"))
//                .execute().actionGet();
//        
////        System.out.println("response = " + response);
//
////        Terms  terms = response.getAggregations().get("distinct_soids");
////        Collection<Terms.Bucket> buckets = terms.getBuckets();
//
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode root = mapper.createObjectNode();
//        try {
//            JsonNode res = mapper.readTree(response.toString());
//            ((ObjectNode)root).put("message", res.get("aggregations").get("distinct_soids").get("buckets"));
//        } catch (Exception e) {
//            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
//        }
//        
//        return root.toString();
    }
    public static String getGroupLastUpdateDocId(String streamId, List<String> soIds) {

            String soId = new String();
            long lastUpdate = 0, max;
            for (String id : soIds) {
                    SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                            .setQuery(QueryBuilders.prefixQuery("meta.id", id + "-" + streamId + "-"))
                            .addAggregation(max("max").field("lastUpdate"))
                            .execute().actionGet();
                    max = (long) ((Max)response.getAggregations().get("max")).getValue();
                    if ( max > lastUpdate ) {
                        lastUpdate = max;
                        soId = id;
                    }
            }
            
            return soId + "-" + streamId + "-" + lastUpdate;
        }
    /*public static String getGroupLastUpdateDocId(String streamId, List<String> soIds) {

        OrFilterBuilder IdsFilter = FilterBuilders.orFilter();
        for(String id : soIds)
            IdsFilter.add(FilterBuilders.prefixFilter("meta.id", id));

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setFrom(0).setSize(1)
                .setQuery(QueryBuilders.regexpQuery("meta.id",".*-" + streamId + "-.*"))
                .setPostFilter(IdsFilter)
                .addSort("doc.lastUpdate", SortOrder.DESC)
                .execute().actionGet();

        if(response.getHits().getTotalHits() > 0)
            return response.getHits().getHits()[0].getId();
        else
            return null;

    }*/

    public static long getLastUpdateTimeStamp(String soId, String streamId) {
        //https://github.com/elasticsearch/elasticsearch/blob/master/src/test/java/org/elasticsearch/search/aggregations/metrics/MaxTests.java

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
                .addSort(SortBuilders.fieldSort("doc.lastUpdate").order(SortOrder.ASC).missing("_last"))
                .addAggregation(max("max").field("lastUpdate"))
                .execute().actionGet();

        Max max = response.getAggregations().get("max");

        return (long)max.getValue();
    }

    public static List<String> getAllUpdatesId(String soId, String streamId) {

        SearchResponse scan; 
        try {
            scan = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                    .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } catch (Exception e) {
            throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        SearchResponse response = client.prepareSearch(soupdates).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
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

//        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
//                .setSearchType(SearchType.SCAN)
//                .setPostFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("doc.callback", "internal")))
//                .setScroll(new TimeValue(60000))
//                .execute().actionGet();
//        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
//                .setPostFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("doc.callback", "internal")))
//                .setSize((int)scan.getHits().getTotalHits())
//                .execute().actionGet();

        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                                       .must(QueryBuilders.termQuery("doc.source", soId))
                                       .must(QueryBuilders.termQuery("doc.stream", streamId))
                                       )
                .setSearchType(SearchType.SCAN)
                .setPostFilter(FilterBuilders.notFilter(FilterBuilders.termFilter("doc.callback", "internal")))
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                                       .must(QueryBuilders.termQuery("doc.source", soId))
                                       .must(QueryBuilders.termQuery("doc.stream", streamId))
                                       )
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

//        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
//                .setSearchType(SearchType.SCAN)
//                .setScroll(new TimeValue(60000))
//                .execute().actionGet();
//
//        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.prefixQuery("meta.id", soId + "-" + streamId + "-"))
//                .setSize((int)scan.getHits().getTotalHits())
//                .execute().actionGet();

        SearchResponse scan = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                                       .must(QueryBuilders.termQuery("doc.source", soId))
                                       .must(QueryBuilders.termQuery("doc.stream", streamId))
                                       )
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .execute().actionGet();

        SearchResponse response = client.prepareSearch(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                                       .must(QueryBuilders.termQuery("doc.source", soId))
                                       .must(QueryBuilders.termQuery("doc.stream", streamId))
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

    public static long getRepeatedSubscriptions(String destination, String source, String callback, String stream) {
        CountResponse response = client.prepareCount(subscriptions).setTypes("couchbaseDocument")
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("doc.destination", destination))
                        .must(QueryBuilders.termQuery("doc.source", source))
                        .must(QueryBuilders.termQuery("doc.callback", callback))
                        .must(QueryBuilders.termQuery("doc.stream", stream))
                        )
                .execute().actionGet();

        // long total_hits = response.getCount();
        return response.getCount();
     }

//    public static List<String> getAllSOS(String userId) {
//
//        SearchResponse scan = client.prepareSearch(serviceobjects).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.boolQuery()
//                		  .should(QueryBuilders.matchQuery("doc.userId", userId))
//                		 )
//                .setSearchType(SearchType.SCAN)
//                .setScroll(new TimeValue(60000))
//                .execute().actionGet();
//
//        SearchResponse response = client.prepareSearch(serviceobjects).setTypes("couchbaseDocument")
//                .setQuery(QueryBuilders.boolQuery()
//                		  .should(QueryBuilders.matchQuery("doc.userId", userId))
//                		 )
//                .setSize((int)scan.getHits().getTotalHits())
//                .execute().actionGet();
//
//        List<String> res = new ArrayList<String>();
//
//        if(response != null) {
//            SearchHits hits = response.getHits();
//            if(hits != null) {
//                long count = hits.getTotalHits();
//                if(count > 0) {
//                    Iterator<SearchHit> iter = hits.iterator();
//                    while(iter.hasNext()) {
//                        SearchHit hit = iter.next();
//                        res.add(hit.getId());
//                    }
//                }
//            }
//        }
//
//        return res;
//    }

}



package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SearchDemo {

    @Test
    public void search() {
        TransportClient client = ElasticsearchUtil.getClient();
        SearchRequestBuilder builder = client.prepareSearch(ElasticsearchUtil.INDEX)
                .setTypes(ElasticsearchUtil.TYPE)
                .setFrom(0)
                .setSize(10);

//        QueryBuilder queryBuilder = QueryBuilders.termQuery("name","张三");

        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("age").gte(10).lte(30);

        builder.setQuery(queryBuilder);

        SearchResponse searchResponse = builder.get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits;
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getId());
        }

    }

    @Test
    public void prepare() throws ExecutionException, InterruptedException {
        TransportClient client = ElasticsearchUtil.getClient();
        long begin = System.currentTimeMillis();
        IndexRequest request = new IndexRequest(ElasticsearchUtil.INDEX+"test", ElasticsearchUtil.TYPE, "010");
        Map<String, Object> source = new HashMap<>();
        source.put("stuNo", "203080110");
        source.put("name", "平安");
        source.put("age", 20);
        source.put("desc", "平安平安平安平安平安平安平安平安平安平安平安平安平安");
        request.source(source);
        System.out.println(Thread.currentThread().getName() + ":main");
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println(Thread.currentThread().getName() + ":onResponse");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(Thread.currentThread().getName() + ":onFailure");
            }
        };
        client.index(request, listener);

        while (true) {

        }

    }


}

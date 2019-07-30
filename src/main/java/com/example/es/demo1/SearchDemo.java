package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.Map;

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
    public void prepare() throws Exception {
        TransportClient client = ElasticsearchUtil.getClient();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(ElasticsearchUtil.INDEX);
        searchRequestBuilder.setFetchSource(true)//是否获取source
                .setFrom(0) //设置分页
                .setSize(10)
                .addSort("stuNo", SortOrder.ASC)
                .addSort("age", SortOrder.DESC);//设置排序

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //filter效率比Query高，filter不计算评分
        //filter:所有filter必须满足
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("age").gte(10).lte(90))
                .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "平安")));
        /*.minimumShouldMatch(1)*/ //当boolQuery中有should设置boolQueryBuilder中should最少满足几个

        //must:全部必须满足
        //boolQueryBuilder.must(QueryBuilders.termQuery());
        //boolQueryBuilder.must(QueryBuilders.termQuery());
        //mustNot:全部必须满足
        //boolQueryBuilder.mustNot(QueryBuilders.termQuery());
        //boolQueryBuilder.mustNot(QueryBuilders.termQuery());
        //should:根据boolQueryBuilder.minimumShouldMatch(int)设置的数量来至少满足几个条件
        //boolQueryBuilder.should(QueryBuilders.termQuery());
        //boolQueryBuilder.should(QueryBuilders.termQuery());

        //matchQuery : 先分词，然后再搜索匹配的词项
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders
//                .matchQuery("fieldName", "context")
//                .operator(Operator.OR)
//                .minimumShouldMatch(String.valueOf(2));

        //matchPhraseQuery
//        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders
//                .matchPhraseQuery("fieldName", "phrase");

        //prefixQuery
//        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("fieldName", "phrase");

        //termQuery
//        TermQueryBuilder termQueryBuilder = QueryBuilders
//                .termQuery("fieldName", "term");

        //termsQuery
//        TermsQueryBuilder termsQueryBuilder = QueryBuilders
//                .termsQuery("fieldName", "term1", "term2", "term3");

        searchRequestBuilder.setQuery(boolQueryBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.totalHits;
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, Object> source = hit.getSource();
            System.out.println("id:" + id + "||" + source);
        }

    }


}

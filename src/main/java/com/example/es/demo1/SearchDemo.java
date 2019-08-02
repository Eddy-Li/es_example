package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SearchDemo {

    @Test
    public void search1() {
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
    public void search2() throws Exception {
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

    //高亮
    @Test
    public void search3() {
        TransportClient client = ElasticsearchUtil.getClient();
        SearchRequestBuilder builder = client.prepareSearch(ElasticsearchUtil.INDEX)
                .setTypes(ElasticsearchUtil.TYPE);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("desc");
        highlightBuilder.preTags("<span style=\\\"color:red\\\">")
                .postTags("</span>")
                .fragmentSize(20);
        builder.highlighter(highlightBuilder);

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("desc", "数据");
        builder.setQuery(termQueryBuilder);

        SearchResponse searchResponse = builder.get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.totalHits;
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String id = searchHit.getId();
            Map<String, Object> source = searchHit.getSource();
            System.out.println("id:" + id + "||" + source);
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            HighlightField highlightField = highlightFields.get("desc");
            Text[] fragments = highlightField.getFragments();
            for (Text fragment : fragments) {
                System.out.println(fragment.toString());
            }
        }
    }

    @Test
    public void search4() throws InterruptedException, ExecutionException {
        TransportClient client = ElasticsearchUtil.getClient();
        SearchRequestBuilder builder = client.prepareSearch(ElasticsearchUtil.INDEX)
                .setTypes(ElasticsearchUtil.TYPE)
                .setScroll(TimeValue.timeValueMinutes(5))
                .setSize(2);

        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        builder.setQuery(query);

        SearchResponse searchResponse = builder.get();
        String scrollId = searchResponse.getScrollId();
        System.out.println(scrollId);
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, Object> source = hit.getSource();
            System.out.println("id:" + id + "||" + source);
        }

        System.out.println(".........................................");
        Thread.sleep(100);
        SearchResponse searchResponse1 = client.prepareSearchScroll(scrollId)
                .setScroll(TimeValue.timeValueMinutes(5))
                .get();
        String scrollId1 = searchResponse1.getScrollId();
        System.out.println(scrollId1);
        System.out.println(scrollId.equals(scrollId1));
        SearchHits searchHits1 = searchResponse1.getHits();
        SearchHit[] hits1 = searchHits1.getHits();
        for (SearchHit hit : hits1) {
            String id = hit.getId();
            Map<String, Object> source = hit.getSource();
            System.out.println("id:" + id + "||" + source);
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        clearScrollRequest.addScrollId(scrollId1);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest).get();
        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println(succeeded);

    }

    @Test
    public void search5(){
        TransportClient client = ElasticsearchUtil.getClient();
        //相当于sql中IS NOT NULL
        ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery("fieldName");
    }


}

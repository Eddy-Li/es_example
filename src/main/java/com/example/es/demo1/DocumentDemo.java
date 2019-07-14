package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DocumentDemo {
    private final String INDEX = "student";
    private final String TYPE = "baseinfo";

    @Test
    public void createDocument() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(INDEX, TYPE, "002");
        Map<String, Object> source = new HashMap<>();
        source.put("stuNo", "203080102");
        source.put("name", "李四");
        source.put("age", 20);
        source.put("desc", "之前使用rest方式调用,不仅在大数据量导入的情况下会有数据丢失的情况,而且编写非常麻烦,就拿mapping举例,全是字符串拼接,一个斜杠少写了就over了,代码看上去很乱.");
        indexRequestBuilder.setSource(source);
        IndexResponse indexResponse = indexRequestBuilder.get();
        System.out.println(indexResponse.status());
    }

    @Test
    public void getDocument() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        GetRequestBuilder getRequestBuilder = transportClient.prepareGet(ElasticsearchUtil.INDEX, ElasticsearchUtil.TYPE, "002");
        GetResponse getResponse = getRequestBuilder.get();
        Map<String, Object> source = getResponse.getSource();
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        String sourceAsString = getResponse.getSourceAsString();
        boolean exists = getResponse.isExists();
        boolean sourceEmpty = getResponse.isSourceEmpty();
    }

    @Test
    public void deleteDocument() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(ElasticsearchUtil.INDEX, ElasticsearchUtil.TYPE, "002");
        DeleteResponse deleteResponse = deleteRequestBuilder.get();
        RestStatus status = deleteResponse.status();//"OK"
    }

    //根据查询条件删除文档
    @Test
    public void deleteDocumentByQuery() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        DeleteByQueryRequestBuilder builder = DeleteByQueryAction
                .INSTANCE
                .newRequestBuilder(transportClient)
                .filter(QueryBuilders.termQuery("name", "王五"))
                .source(ElasticsearchUtil.INDEX);
        BulkByScrollResponse response = builder.get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    @Test
    public void updateDocument() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        UpdateRequestBuilder updateRequestBuilder = transportClient.prepareUpdate(ElasticsearchUtil.INDEX, ElasticsearchUtil.TYPE, "002");
        Map<String, Object> source = new HashMap<>();
        source.put("name", "王五");
        updateRequestBuilder.setDoc(source);
        UpdateResponse updateResponse = updateRequestBuilder.get();
        System.out.println(updateResponse);
    }

    //一次获取多个文档
    @Test
    public void multiGet() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        MultiGetRequestBuilder multiGetRequestBuilder = transportClient.prepareMultiGet();
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder
                .add(ElasticsearchUtil.INDEX, ElasticsearchUtil.TYPE, "001", "002")
//                .add(ElasticsearchUtil.INDEX2, ElasticsearchUtil.TYPE2,idList)
                .get();
        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            GetResponse response = multiGetItemRespons.getResponse();
            if (response != null && response.isExists()) {
                System.out.println(response.getSource());
            }
        }
    }


}

package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.util.Map;

public class DocumentDemo {
    private final String INDEX = "student";
    private final String TYPE = "baseinfo";

    @Test
    public void getDocument() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        GetRequestBuilder getRequestBuilder = transportClient.prepareGet(INDEX, TYPE, "002");
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
        DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(INDEX, TYPE, "002");
        DeleteResponse deleteResponse = deleteRequestBuilder.get();
        RestStatus status = deleteResponse.status();
    }

}

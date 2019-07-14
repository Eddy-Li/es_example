package com.example.es.demo1;

import com.example.es.util.ElasticsearchUtil;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.get.GetField;
import org.junit.Test;

import java.util.Map;

public class GetDocumentDemo {
    private final String INDEX = "student";
    private final String TYPE = "baseinfo";

    @Test
    public void test() {
        TransportClient transportClient = ElasticsearchUtil.getClient();
        GetRequestBuilder getRequestBuilder = transportClient.prepareGet(INDEX, TYPE, "002");
        GetResponse getResponse = getRequestBuilder.get();
        Map<String, Object> source = getResponse.getSource();
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        String sourceAsString = getResponse.getSourceAsString();
        boolean exists = getResponse.isExists();
        boolean sourceEmpty = getResponse.isSourceEmpty();
    }
}

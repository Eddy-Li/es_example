package com.example.es.demo1;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class IndexDemo {

    private final String INDEX = "student";
    private final String TYPE = "baseinfo";

    private TransportClient client;

    @Before
    public void before() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .put("client.transport.sniff", true)
                .build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.137.10"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.137.20"), 9300));
    }

    @Test
    public void createIndex() {
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(INDEX, TYPE, "002");
        Map<String, Object> source = new HashMap<>();
        source.put("stuNo","203080102");
        source.put("name","李四");
        source.put("age",20);
        source.put("desc","之前使用rest方式调用,不仅在大数据量导入的情况下会有数据丢失的情况,而且编写非常麻烦,就拿mapping举例,全是字符串拼接,一个斜杠少写了就over了,代码看上去很乱.");
        indexRequestBuilder.setSource(source);
        IndexResponse indexResponse = indexRequestBuilder.get();
        System.out.println(indexResponse.status());
    }
}

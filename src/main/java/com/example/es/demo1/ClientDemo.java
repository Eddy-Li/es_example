package com.example.es.demo1;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class ClientDemo {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .put("client.transport.sniff", true)
                .put("thread_pool.search.size", 20) //设置搜索线程池个数
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.137.10"),9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.137.20"),9300));

        System.out.println(client);
    }
}

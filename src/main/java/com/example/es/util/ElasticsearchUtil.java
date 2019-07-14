package com.example.es.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class ElasticsearchUtil {
    private final static Logger logger = LoggerFactory.getLogger(ElasticsearchUtil.class);

    private static TransportClient transportClient;

    public static final String INDEX = "student";
    public static final String TYPE = "baseinfo";

    private ElasticsearchUtil() {
    }

    private static Properties getProperties() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("es.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("ElasticsearchUtil: load es.properties file fail...");
        }
        return properties;
    }

    public static TransportClient getClient() {
        if (transportClient == null) {
            initTransportClient();
        }
        logger.info("ElasticsearchUtil: get client success");
        return transportClient;
    }


    private static synchronized void initTransportClient() {
        if (transportClient == null) {
            Properties properties = getProperties();
            Settings settings = Settings.builder()
                    .put("cluster.name", properties.getProperty("cluster.name"))
                    .put("client.transport.sniff", Boolean.valueOf(properties.getProperty("client.transport.sniff")))
                    .put("thread_pool.search.size", Integer.valueOf(properties.getProperty("thread_pool.search.size"))) //设置搜索线程池个数
                    .build();
            transportClient = new PreBuiltTransportClient(settings);
            try {
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(properties.getProperty("es.ip1")), Integer.valueOf(properties.getProperty("es.port1"))))
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(properties.getProperty("es.ip2")), Integer.valueOf(properties.getProperty("es.port2"))));
                logger.info("ElasticsearchUtil: initTransportClient success");
            } catch (UnknownHostException e) {
                logger.error("ElasticsearchUtil: ip or port in es.properties have format error...");
                e.printStackTrace();
            }
        }
    }


}

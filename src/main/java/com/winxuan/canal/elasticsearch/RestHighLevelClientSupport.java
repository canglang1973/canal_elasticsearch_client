package com.winxuan.canal.elasticsearch;

import com.google.common.collect.Lists;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description:
 **/
public class RestHighLevelClientSupport {

    private String address;
    private int port;

    public RestHighLevelClientSupport(String address, int port) {
        this.address = address;
        this.port = port;
    }


    public String getAddress() {
        return address;
    }

    public RestHighLevelClient getTransportClient() {

        List<HttpHost> transportAddresses = Lists.newArrayList();
        try {
            if (getAddress().contains(",")) {
                for (String ip : getAddress().split(",")) {
                    transportAddresses.add(new HttpHost(InetAddress.getByName(ip.trim()), port));
                }
            } else {
                transportAddresses.add(new HttpHost(InetAddress.getByName(getAddress()), port));
            }

            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(transportAddresses.toArray(new HttpHost[transportAddresses.size()])));

            return client;
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}

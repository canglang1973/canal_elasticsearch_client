package com.winxuan.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.winxuan.canal.config.CanalElasticsearchClientConfig;

import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: 单机模式客户端
 **/
public class SimpleCanalClient extends BaseCanalClient {

    public SimpleCanalClient(String destination) {
        super(destination);
    }

    public static void main(String[] args) throws Exception {
        // 根据ip，直接创建链接，无HA的功能
        CanalElasticsearchClientConfig config = new CanalElasticsearchClientConfig();
        config.loadProps(args);
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.SINGLE_SERVER_HOST)
                        , CanalElasticsearchClientConfig.getSingleCanalServerPort()),
                CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION), "", "");
        final SimpleCanalClient client = new SimpleCanalClient(CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION));
        client.setConnector(connector);
        client.start();
        logger.info("## canal simple client start succuss ....");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                client.stop();
            } catch (Throwable e) {
                logger.warn("## something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }

}

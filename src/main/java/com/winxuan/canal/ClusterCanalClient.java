package com.winxuan.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.winxuan.canal.config.CanalElasticsearchClientConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: 集群模式客户端
 **/
public class ClusterCanalClient extends BaseCanalClient {

    public ClusterCanalClient(String destination) {
        super(destination);
    }

    public static void main(String[] args) throws Exception {
        CanalElasticsearchClientConfig config = new CanalElasticsearchClientConfig();
        config.loadProps(args);
        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        ClusterCanalClient clusterCanalClient = new ClusterCanalClient(CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION));
        CanalConnector connector = CanalConnectors.newClusterConnector(
                CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ZK_SERVERS),
                CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION), "", "");
        clusterCanalClient.setConnector(connector);
        clusterCanalClient.start();
        logger.info("## canal cluster client start succuss ....");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the cluster canal client");
                clusterCanalClient.stop();
            } catch (Throwable e) {
                logger.warn("## something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## cluster canal client is down.");
            }
        }));
    }
}

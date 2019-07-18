package com.winxuan.canal.config;

import ch.qos.logback.classic.Level;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author leitao.
 * @category
 * @time: 2019/7/3 0003-16:50
 * @version: 1.0
 * @description: 客户端配置加载
 **/
public class CanalElasticsearchClientConfig {

    protected final static Logger logger = LoggerFactory.getLogger(CanalElasticsearchClientConfig.class);

    private static final Map<String, String> props = new ConcurrentHashMap<>();

    /**
     * 默认客户端配置文件 文件名
     */
    public static final String DEFAULT_PROPERTIES_FILE_NAME = "local_db.properties";

    /**
     * 客户端默认配置文件夹
     */
    public static final String DEFAULT_CONFIG_DIR = "conf";

    /**
     * zk机器列表,ex:127.0.0.1:2181,127.0.0.2:2181
     */
    public static final String ZK_SERVERS = "zk.servers";

    public static final String DESTINATION = "destination";
    public static final String CANAL_BATCH_SIZE = "canal.batch.size";

    /**
     * ES集群配置
     */
    public static final String ES_CLUSTER_NAME = "target.es.cluster.name";
    public static final String ES_CLUSTER_ADDRESSES = "target.es.cluster.addresses";
    public static final String ES_CLUSTER_PORT = "target.es.cluster.port";

    /**
     * ES 批量请求配置
     * 每添加ES_REQUEST_BULK_ACTIONS 个request，执行一次bulk操作
     */
    public static final String ES_REQUEST_BULK_ACTIONS = "es.request.bulk.actions";

    /**
     * 数据每达到ES_REQUEST_BULK_SIZE MB的请求size时，执行一次bulk操作,单位:MB
     */
    public static final String ES_REQUEST_BULK_SIZE = "es.request.bulk.size";
    /**
     * 默认是1，表示积累bulk requests和发送bulk是异步的，其数值表示发送bulk的并发线程数，设置为0表示二者同步的
     */
    public static final String ES_REQUEST_BULK_CONCURRENTREQUESTS = "es.request.bulk.concurrentrequests";
    /**
     * 每 ES_REQUEST_BULK_FLUSHINTERVAL 秒执行一次bulk操作
     */
    public static final String ES_REQUEST_BULK_FLUSHINTERVAL = "es.request.bulk.flushinterval";
    /**
     * 当ES由于资源不足发生异常重试 延迟 单位:毫秒
     */
    public static final String ES_REQUEST_BULK_BACKOFFPOLICY_DELAY = "es.request.bulk.backoffpolicy.delay";
    /**
     * 当ES由于资源不足发生异常重试 次数
     */
    public static final String ES_REQUEST_BULK_BACKOFFPOLICY_RETRIES = "es.request.bulk.backoffpolicy.retries";

    /**
     * 直连canal server 配置
     */
    public static final String SINGLE_SERVER_HOST = "canal.single.server.host";
    /**
     * 默认端口 11111
     */
    public static final String SINGLE_SERVER_PORT = "canal.single.server.port";
    public static final int DEFAULT_SINGLE_SERVER_PORT = 11111;

    /**
     * 日志打印级别
     */
    public static final String LOG_LEVEL = "canal.client.log.level";

    /**
     * 加载 local_db.properties 配置文件
     */
    public void loadProps(String[] args) throws Exception {
        String fileName = null != args && args.length == 1 ? args[0] : DEFAULT_PROPERTIES_FILE_NAME;
        try {
            File configFile = new File(DEFAULT_CONFIG_DIR + File.separator + fileName);
            //加载conf目录下用户自己定义的属性文件,文件格式 key=value
            if (configFile.exists()) {
                logger.info("Reading user-defined config start from " + configFile.getAbsolutePath());
                List<String> strings = FileUtils.readLines(configFile);
                strings.stream().filter(s -> StringUtils.isNotEmpty(s) && !s.startsWith("#") && s.contains("=")).forEach(s -> {
                    String[] split = s.split("=");
                    if (split.length == 2 && StringUtils.isNotEmpty(split[0]) && StringUtils.isNotEmpty(split[1])) {
                        logger.info(String.format("Reading user-defined config key[%s],value[%s]", split[0], split[1]));
                        CanalElasticsearchClientConfig.setConfig(split[0].trim(), split[1].trim());
                    }
                });
                logger.info("Reading user-defined config end from " + configFile.getAbsolutePath());
            } else {
                //如果用户自定义的配置文件不存在则读取jar下默认的处理client.properties
                InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName);
                logger.info("Reading default config start from " + fileName);
                Properties fileProps = new Properties();
                fileProps.load(in);
                for (Object key : fileProps.keySet()) {
                    String value = (String) fileProps.get(key);
                    if (StringUtils.isNotEmpty(value)) {
                        CanalElasticsearchClientConfig.setConfig((String) key, (String) fileProps.get(key));
                        logger.info(String.format("Reading default config key[%s],value[%s]", key, value));
                    }
                }
                logger.info("Reading default config end " + fileName);
            }
            //设置日志级别
            configLogLevel();
        } catch (IOException e) {
            logger.error("Reading config error " + e.getMessage(), e);
            throw e;
        }
        if (!fileName.split("\\.")[0].equals(CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION))) {
            String errmsg = "Reading config error config fileName " + fileName + " is not equal to destination " +
                    CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.DESTINATION);
            logger.error(errmsg);
            throw new Exception(errmsg);
        }
    }

    public static String getConfig(String key) {
        Preconditions.checkNotNull(key, "key cannot be null");
        return props.get(key);
    }

    public static void setConfig(String key, String value) {
        Preconditions.checkNotNull(key, "key cannot be null");
        Preconditions.checkNotNull(value, "value cannot be null");
        props.put(key, value);
    }

    public static String removeConfig(String key) {
        Preconditions.checkNotNull(key, "key cannot be null");
        return props.remove(key);
    }

    public static int getESClusterPort() {
        return Integer.parseInt(getConfig(ES_CLUSTER_PORT));
    }

    public static int getSingleCanalServerPort() {
        String port = getConfig(SINGLE_SERVER_PORT);
        if (StringUtils.isEmpty(port)) {
            return DEFAULT_SINGLE_SERVER_PORT;
        }
        return Integer.parseInt(port);
    }

    private static void configLogLevel() {
        final org.slf4j.Logger logger = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        if (!(logger instanceof ch.qos.logback.classic.Logger)) {
            return;
        }
        ((ch.qos.logback.classic.Logger) logger).setLevel(Level.toLevel(getConfig(LOG_LEVEL), Level.INFO));
        logger.info("canal client lo level : " + ((ch.qos.logback.classic.Logger) logger).getLevel().levelStr);
    }
}

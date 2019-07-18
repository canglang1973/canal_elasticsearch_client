package com.winxuan.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Preconditions;
import com.winxuan.canal.config.CanalElasticsearchClientConfig;
import com.winxuan.canal.elasticsearch.ElasticsearchBulkProcessor;
import com.winxuan.canal.support.Constants;
import com.winxuan.canal.support.Dml;
import com.winxuan.canal.support.MessageUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: 客户端基类
 **/
public class BaseCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(BaseCanalClient.class);

    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    protected volatile boolean running = false;
    protected Thread messageProcessThread = null;
    protected String destination;
    protected CanalConnector connector;
    private ElasticsearchBulkProcessor bulkProcessor;
    private int defaultBatchSize = 1000;

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public BaseCanalClient(String destination) {
        this(destination, null);
    }

    public BaseCanalClient(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Preconditions.checkNotNull(connector, "connector is null");
        bulkProcessor = new ElasticsearchBulkProcessor();
        messageProcessThread = new Thread(() -> process());
        messageProcessThread.setUncaughtExceptionHandler(handler);
        running = true;
        messageProcessThread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (messageProcessThread != null) {
            try {
                messageProcessThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        MDC.remove(CanalElasticsearchClientConfig.DESTINATION);
    }

    protected void process() {
        String batchSizeStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.CANAL_BATCH_SIZE);
        int batchSize = StringUtils.isNotEmpty(batchSizeStr) && StringUtils.isNumeric(batchSizeStr) ? Integer.parseInt(batchSizeStr) : defaultBatchSize;
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                logger.info("已连接到canal server");
                while (running) {
                    //获取指定数量的数据
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    try {
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                        } else {
                            List<Dml> dmls = MessageUtil.parse4Dml(message);
                            if (CollectionUtils.isNotEmpty(dmls)) {
                                //只有当有修改数据的时候才同步到ES
                                logger.debug(String.format("batchId:%s;size:%s", batchId, dmls.size()));
                                //解析数据并提交ES请求
                                this.canalToElasticsearch(dmls);
                            }
                            connector.ack(batchId);
                        }
                    } catch (Exception e) {
                        connector.rollback();
                        logger.error(String.format("canal client get message error rollback batchId:%s,errorMsg:%s", batchId, e.getMessage()), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
                try {
                    //睡5s后重新尝试连canal server
                    Thread.sleep(5000L);
                } catch (InterruptedException e1) {
                }
            } finally {
                connector.disconnect();
            }
            logger.info("正在尝试重新连接canal server...");
        }
    }

    private BulkProcessor getBulkProcessor() {
        return this.bulkProcessor.getBulkProcessor();
    }

    private void canalToElasticsearch(List<Dml> dmls) {
        for (Dml dml : dmls) {
            String tableName = dml.getTable();
            if (CanalEntry.EventType.INSERT.name().equals(dml.getType())) {
                dml.getData().stream().filter(map -> map.get(Constants.PK_ID) != null).forEach(map -> {
                    String id = map.get(Constants.PK_ID).toString();
                    this.handleColumnData(tableName, map);
                    //存在就更新,不存在就插入
                    this.getBulkProcessor().add(new IndexRequest(tableName, Constants.ES_DOC_TYPE, id).source(JSON.parseObject(JSON.toJSONString(map))));
                });
                continue;
            }
            if (CanalEntry.EventType.UPDATE.name().equals(dml.getType())) {
                dml.getData().stream().filter(map -> map.get(Constants.PK_ID) != null).forEach(map -> {
                    String id = map.get(Constants.PK_ID).toString();
                    this.handleColumnData(tableName, map);
                    //如果id相同则是先删除文档再插入
                    this.getBulkProcessor().add(new UpdateRequest(tableName, Constants.ES_DOC_TYPE, id).docAsUpsert(true).doc(JSON.parseObject(JSON.toJSONString(map))));
                });
                continue;
            }
            if (CanalEntry.EventType.DELETE.name().equals(dml.getType())) {
                dml.getData().stream().filter(map -> map.get(Constants.PK_ID) != null).forEach(map -> {
                    //删除指定id的文档
                    this.getBulkProcessor().add(new DeleteRequest(tableName, Constants.ES_DOC_TYPE, map.get(Constants.PK_ID).toString()));
                });
            }
        }
    }

    /**
     * 处理不同数据类型的列的数据
     *
     * @param tableName
     * @param map
     */
    private void handleColumnData(String tableName, Map<String, Object> map) {
        this.handleSQLTimestamp(map);
        this.handleBlobText(tableName, map);
        this.handleBlobColumn(map);
        logger.debug(String.format("表[%s],变更后的数据:%s", tableName, JSON.toJSONString(map)));
        //id已作为ES文档的_id 此处文档内就不重复存id字段了
        map.remove(Constants.PK_ID);
    }

    /**
     * 格式化时间
     *
     * @param map
     */
    private void handleSQLTimestamp(Map<String, Object> map) {
        map.entrySet().stream().filter(entry -> entry.getValue() instanceof java.sql.Timestamp).forEach(
                entry -> {
                    java.sql.Timestamp timestamp = (Timestamp) entry.getValue();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Constants.ES_DATEFORMAT_PATTERN);
                    simpleDateFormat.setTimeZone(TimeZone.getTimeZone(Constants.UTC_TIME_ZONE));
                    String format = simpleDateFormat.format(timestamp);
                    map.put(entry.getKey(), format);
                }
        );
    }

    /**
     * 解压shop_item_text 表中的大字段
     *
     * @param tableName
     * @param map
     */
    private void handleBlobText(String tableName, Map<String, Object> map) {
        if (Constants.SHOP_ITEM_TEXT_TABLE.equals(tableName)) {
            map.entrySet().stream().filter(entry -> Constants.SHOP_ITEM_TEXT_BLOB_COLUMN.equals(entry.getKey())).forEach(entry -> {
                byte[] textBytes = (byte[]) entry.getValue();
                String text = null;
                try {
                    text = Snappy.uncompressString(textBytes);
                } catch (Exception e) {
                    logger.error(String.format("Snappy解压表[%s],列[%s]异常,ID[%s]", tableName, entry.getKey(), map.get(Constants.PK_ID)), e);
                }
                map.put(entry.getKey(), text);
            });
        }
    }

    /**
     * 将二进制数据转换成字符串
     *
     * @param map
     */
    private void handleBlobColumn(Map<String, Object> map) {
        map.entrySet().stream().filter(entry -> entry.getValue() instanceof byte[]).forEach(
                entry -> map.put(entry.getKey(), new String((byte[]) entry.getValue(), Charset.defaultCharset()))
        );
    }


}

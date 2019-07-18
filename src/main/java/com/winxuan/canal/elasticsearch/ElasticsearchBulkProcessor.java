package com.winxuan.canal.elasticsearch;

import com.google.common.base.Throwables;
import com.winxuan.canal.config.CanalElasticsearchClientConfig;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author leitao.
 * @category
 * @time: 2019/7/8 0008-15:27
 * @version: 1.0
 * @description:
 **/
public class ElasticsearchBulkProcessor {

    protected final static Logger logger = LoggerFactory.getLogger(ElasticsearchBulkProcessor.class);

    private RestHighLevelClient client;

    private BulkProcessor bulkProcessor;

    private int bulkActions = 100;
    private long bulkSize = 3;
    private int concurrentRequests = 1;
    private long flushInterval = 1;
    private int backoffPolicyDelay = 1000;
    private int backoffPolicyRetries = 3;


    private BulkProcessor prepareElasticsearchBulkProcessor() {
        client = new RestHighLevelClientSupport(CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_CLUSTER_ADDRESSES),
                CanalElasticsearchClientConfig.getESClusterPort()).getTransportClient();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                logger.info("Bulk processor start");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                logger.info("Bulk processor end");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                logger.error("Bulk processor error: " + Throwables.getStackTraceAsString(throwable));
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
        //每添加10000个request，执行一次bulk操作
        builder.setBulkActions(this.getBulkActions());
        //每达到1G的请求size时，执行一次bulk操作
        builder.setBulkSize(new ByteSizeValue(this.getBulkSize(), ByteSizeUnit.MB));
        //默认是1，表示积累bulk requests和发送bulk是异步的，其数值表示发送bulk的并发线程数，设置为0表示二者同步的
        builder.setConcurrentRequests(this.getConcurrentRequests());
        //每3s执行一次bulk操作
        builder.setFlushInterval(TimeValue.timeValueSeconds(this.getFlushInterval()));
        //当ES由于资源不足发生异常重试
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(this.getBackoffPolicyDelay()), this.getBackoffPolicyRetries()));
        return builder.build();
    }

    public BulkProcessor getBulkProcessor() {
        if (null == bulkProcessor) {
            BulkProcessor bulkProcessor = prepareElasticsearchBulkProcessor();
            this.bulkProcessor = bulkProcessor;
        }
        return bulkProcessor;
    }

    public int getBulkActions() {
        String bulkActionsStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_ACTIONS);
        if (StringUtils.isNotEmpty(bulkActionsStr) && StringUtils.isNumeric(bulkActionsStr = bulkActionsStr.trim())) {
            bulkActions = Integer.parseInt(bulkActionsStr);
        }
        return bulkActions;
    }

    public long getBulkSize() {
        String bulkSizeStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_SIZE);
        if (StringUtils.isNotEmpty(bulkSizeStr) && StringUtils.isNumeric(bulkSizeStr = bulkSizeStr.trim())) {
            bulkSize = Long.parseLong(bulkSizeStr);
        }
        return bulkSize;
    }

    public int getConcurrentRequests() {
        String concurrentRequestsStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_CONCURRENTREQUESTS);
        if (StringUtils.isNotEmpty(concurrentRequestsStr) && StringUtils.isNumeric(concurrentRequestsStr = concurrentRequestsStr.trim())) {
            concurrentRequests = Integer.parseInt(concurrentRequestsStr);
        }
        return concurrentRequests;
    }

    public long getFlushInterval() {
        String flushIntervalStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_FLUSHINTERVAL);
        if (StringUtils.isNotEmpty(flushIntervalStr) && StringUtils.isNumeric(flushIntervalStr = flushIntervalStr.trim())) {
            flushInterval = Long.parseLong(flushIntervalStr);
        }
        return flushInterval;
    }

    public int getBackoffPolicyDelay() {
        String backoffPolicyDelayStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_BACKOFFPOLICY_DELAY);
        if (StringUtils.isNotEmpty(backoffPolicyDelayStr) && StringUtils.isNumeric(backoffPolicyDelayStr = backoffPolicyDelayStr.trim())) {
            backoffPolicyDelay = Integer.parseInt(backoffPolicyDelayStr);
        }
        return backoffPolicyDelay;
    }

    public int getBackoffPolicyRetries() {
        String backoffPolicyRetriesStr = CanalElasticsearchClientConfig.getConfig(CanalElasticsearchClientConfig.ES_REQUEST_BULK_BACKOFFPOLICY_RETRIES);
        if (StringUtils.isNotEmpty(backoffPolicyRetriesStr) && StringUtils.isNumeric(backoffPolicyRetriesStr = backoffPolicyRetriesStr.trim())) {
            backoffPolicyRetries = Integer.parseInt(backoffPolicyRetriesStr);
        }
        return backoffPolicyRetries;
    }
}

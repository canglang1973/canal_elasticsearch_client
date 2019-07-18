Canal client

扩展canal client 接收canal server 消息批量存入Elasticsearch;

配置:

1.HA集群 zk.servers 必填 将client注册到zk上;

2.配置文件存放在与jar同级目录conf下,文件格式为*.propertoes,文件名*必须与要监听的destination名称一样;

3.启动main方法时传入参数为配置文件名;

单机启动:

    #!/bin/bash
    nohup nohup /opt/app/jdk1.8.0_45/bin/java -cp canal_elasticsearch_client.jar com.winxuan.canal.SimpleCanalClient \
    shop_item.properties \
    > shop_item_simple.out 2>&1 &

集群启动:

    #!/bin/bash
    nohup nohup /opt/app/jdk1.8.0_45/bin/java -cp canal_elasticsearch_client.jar com.winxuan.canal.ClusterCanalClient \
    shop_item.properties \
    > shop_item_cluster.out 2>&1 &

配置文件内容:

    # client 配置
    zk.servers=127.0.0.1:2181
    
    # 直连canal server
    canal.single.server.host=127.0.0.1
    canal.single.server.port=11111
    canal.batch.size=5120
    canal.client.log.level=INFO
        
    destination=shop_item
    
    # 同步目标: es 配置
    target.es.cluster.name=es-cluster
    target.es.cluster.addresses=127.0.0.1
    target.es.cluster.port=9200
    
    # 批量ES请求配置 不配置按系统默认值
    es.request.bulk.actions = 1000
    es.request.bulk.size=30
    es.request.bulk.concurrentrequests=1
    es.request.bulk.flushinterval=10
    es.request.bulk.backoffpolicy.delay=10000
    es.request.bulk.backoffpolicy.retries=3

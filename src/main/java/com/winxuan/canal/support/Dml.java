package com.winxuan.canal.support;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: DML操作转换对象
 **/
public class Dml implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    /**
     * 对应canal的实例
     */
    private String destination;
    /**
     * 对应mq的group id
     */
    private String groupId;
    /**
     * 数据库或schema
     */
    private String database;
    /**
     * 表名
     */
    private String table;
    /**
     * 主键字段
     */
    private List<String> pkNames;

    private Boolean isDdl;
    /**
     * 类型: INSERT UPDATE DELETE
     */
    private String type;
    /**
     * 执行耗时
     */
    private Long es;
    /**
     * 同步时间
     */
    private Long ts;
    /**
     * 执行的sql, dml sql为空
     */
    private String sql;
    /**
     * 数据列表
     */
    private List<Map<String, Object>> data;
    /**
     * 旧数据列表, 用于update, size和data的size一一对应
     */
    private List<Map<String, Object>> old;

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        es = null;
        data = null;
        old = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "Dml{" + "destination='" + destination + '\'' + ", database='" + database + '\'' + ", table='" + table
                + '\'' + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\'' + ", data="
                + data + ", old=" + old + '}';
    }
}

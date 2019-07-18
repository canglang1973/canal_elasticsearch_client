package com.winxuan.canal.support;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.util.*;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: Message对象解析工具类
 **/
public class MessageUtil {

    public static List<Dml> parse4Dml(Message message) {
        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<Dml> dmls = new ArrayList<Dml>(entries.size());
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            Dml dml = new Dml();
            dml.setIsDdl(rowChange.getIsDdl());
            dml.setDatabase(entry.getHeader().getSchemaName());
            dml.setTable(entry.getHeader().getTableName());
            dml.setType(eventType.toString());
            dml.setEs(entry.getHeader().getExecuteTime());
            dml.setIsDdl(rowChange.getIsDdl());
            dml.setTs(System.currentTimeMillis());
            dml.setSql(rowChange.getSql());
            dmls.add(dml);
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                dml.setPkNames(new ArrayList<>());
                int i = 0;
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    for (CanalEntry.Column column : columns) {
                        if (i == 0) {
                            if (column.getIsKey()) {
                                dml.getPkNames().add(column.getName());
                            }
                        }
                        if (column.getIsNull()) {
                            row.put(column.getName(), null);
                        } else {
                            row.put(column.getName(),
                                    JdbcTypeUtil.typeConvert(dml.getTable(),
                                            column.getName(),
                                            column.getValue(),
                                            column.getSqlType(),
                                            column.getMysqlType()));
                        }
                        // 获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    if (!row.isEmpty()) {
                        data.add(row);
                    }
                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, Object> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                if (column.getIsNull()) {
                                    rowOld.put(column.getName(), null);
                                } else {
                                    rowOld.put(column.getName(),
                                            JdbcTypeUtil.typeConvert(dml.getTable(),
                                                    column.getName(),
                                                    column.getValue(),
                                                    column.getSqlType(),
                                                    column.getMysqlType()));
                                }
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }
                    i++;
                }
                if (!data.isEmpty()) {
                    dml.setData(data);
                }
                if (!old.isEmpty()) {
                    dml.setOld(old);
                }
            }
        }

        return dmls;
    }
}

package com.winxuan.canal.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author leitao.
 * @category
 * @time: 2019/6/21 0021-10:18
 * @version: 1.0
 * @description: 类型转换工具类
 **/
public class JdbcTypeUtil {

    private static Logger logger = LoggerFactory.getLogger(JdbcTypeUtil.class);

    private static boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
                || "TEXT".equalsIgnoreCase(columnType) || "TINYTEXT".equalsIgnoreCase(columnType);
    }

    public static Object typeConvert(String tableName, String columnName, String value, int sqlType, String mysqlType) {
        if (value == null
                || (value.equals("") && !(isText(mysqlType) || sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.LONGVARCHAR))) {
            return null;
        }
        try {
            Object res;
            switch (sqlType) {
                case Types.INTEGER:
                    res = Integer.parseInt(value);
                    break;
                case Types.SMALLINT:
                    res = Short.parseShort(value);
                    break;
                case Types.BIGINT:
                    if (mysqlType.startsWith("bigint") && mysqlType.endsWith("unsigned")) {
                        res = new BigInteger(value);
                    } else {
                        res = Long.parseLong(value);
                    }
                    break;
                case Types.BIT:
                case Types.TINYINT:
                case Types.BOOLEAN:
                    res = !"0".equals(value);
                    break;
                case Types.DOUBLE:
                case Types.FLOAT:
                    res = Double.parseDouble(value);
                    break;
                case Types.REAL:
                    res = Float.parseFloat(value);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    res = new BigDecimal(value);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    res = value.getBytes("ISO-8859-1");
                    break;
                case Types.DATE:
                    if (!value.startsWith("0000-00-00")) {
                        java.util.Date date = Util.parseDate(value);
                        if (date != null) {
                            res = new Date(date.getTime());
                        } else {
                            res = null;
                        }
                    } else {
                        res = null;
                    }
                    break;
                case Types.TIME: {
                    java.util.Date date = Util.parseDate(value);
                    if (date != null) {
                        res = new Time(date.getTime());
                    } else {
                        res = null;
                    }
                    break;
                }
                case Types.TIMESTAMP:
                    if (!value.startsWith("0000-00-00")) {
                        java.util.Date date = Util.parseDate(value);
                        if (date != null) {
                            res = new Timestamp(date.getTime());
                        } else {
                            res = null;
                        }
                    } else {
                        res = null;
                    }
                    break;
                case Types.CLOB:
                default:
                    res = value;
                    break;
            }
            return res;
        } catch (Exception e) {
            logger.error("table: {} column: {}, failed convert type {} to {}", tableName, columnName, value, sqlType);
            return value;
        }
    }
}

package com.dtstack.flinkx.binlog.writerutil;

import org.apache.commons.lang.StringUtils;
import java.util.*;

/**
 * @author 张成明
 * @version 1.0
 * @date 2021/6/18 14:55
 * @explain
 */
public class TableSqlUtil {

    public static String getInsertStatement(List<String> column, String table) {
        return "INSERT INTO " + quoteTable(table)
                + " (" + quoteColumns(column) + ") values ("
                + StringUtils.repeat("?", ",", column.size()) + ")";
    }
    public static String  quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if(i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }
    public static String getStartQuote() {
        return "\"";
    }
    public static String getEndQuote() {
        return "\"";
    }

    public static String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for(String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }
    public static String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }


    public static String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }


}

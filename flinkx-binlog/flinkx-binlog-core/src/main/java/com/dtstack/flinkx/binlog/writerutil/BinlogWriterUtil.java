/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.binlog.writerutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;



/**
 * @author toutian
 */
public class BinlogWriterUtil {

    private static Logger logger = LoggerFactory.getLogger(BinlogWriterUtil.class);

    public static final String LEFT_BRACKETS = "(";

    private final List<String> tableExistException = Arrays.asList("TableExistsException", "AlreadyExistsException", "TableAlreadyExistsException");

    public final static String TABLE_COLUMN_KEY = "key";
    public final static String TABLE_COLUMN_TYPE = "type";
    public final static String PARTITION_TEMPLATE = "%s=%s";

    private BinlogWriterDbUtil.ConnectionInfo connectionInfo;

    enum HiveReleaseVersion{
        /**
         * apache hive 1.x
         */
        APACHE_1("apache", "1"),

        /**
         * apache hive 2.x
         */
        APACHE_2("apache", "2"),

        /**
         * cdh hive 1.x
         */
        CDH_1("cdh", "1"),

        /**
         * cdh hive 2.x
         */
        CDH_2("cdh", "2");

        private String name;

        private String version;

        HiveReleaseVersion(String name, String version) {
            this.name = name;
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    public BinlogWriterUtil() {
    }

    /**
     * 抛出异常,直接终止hive
     */
    public BinlogWriterUtil(BinlogWriterDbUtil.ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }



    private boolean isTableExistsException(String message){
        if (message == null) {
            return false;
        }

        for (String msg : tableExistException) {
            if (message.contains(msg)) {
                return true;
            }
        }

        return false;
    }

    public static String getCreateTableHql(TableInfo tableInfo) {
        //不要使用create table if not exist，可能以后会在业务逻辑中判断表是否已经存在
        StringBuilder fieldsb = new StringBuilder("CREATE TABLE %s (");
        for (int i = 0; i < tableInfo.getColumns().size(); i++) {
            fieldsb.append(String.format("`%s` %s", tableInfo.getColumns().get(i), tableInfo.getColumnTypes().get(i)));
            if (i != tableInfo.getColumns().size() - 1) {
                fieldsb.append(",");
            }
        }
        fieldsb.append(") ");
        if (!tableInfo.getPartitions().isEmpty()) {
            fieldsb.append(" PARTITIONED BY (");
            for (String partitionField : tableInfo.getPartitions()) {
                fieldsb.append(String.format("`%s` string", partitionField));
            }
            fieldsb.append(") ");
        }
        if (EStoreType.TEXT.name().equalsIgnoreCase(tableInfo.getStore())) {
            fieldsb.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
            fieldsb.append(tableInfo.getDelimiter());
            fieldsb.append("' LINES TERMINATED BY '\\n' STORED AS TEXTFILE ");
        } else if(EStoreType.ORC.name().equalsIgnoreCase(tableInfo.getStore())) {
            fieldsb.append(" STORED AS ORC ");
        }else{
            fieldsb.append(" STORED AS PARQUET ");
        }
        return fieldsb.toString();
    }

    public static String getHiveColumnType(String originType) {
        originType = originType.trim();
        int indexOfBrackets = originType.indexOf(LEFT_BRACKETS);
        if (indexOfBrackets > -1) {
            String params = originType.substring(indexOfBrackets);
            int index = params.indexOf(",");
            int right = Integer.parseInt(params.substring(index+1, params.length()-1).trim());
            if(right == 0){
                int left = Integer.parseInt(params.substring(1, index).trim());
                if(left <= 4){
                    return "SMALLINT";
                }else if(left <= 9){
                    return "INT";
                }else if(left <= 18){
                    return "BIGINT";
                }
            }
            return "DECIMAL" + params;
        } else {
            return convertType(originType);
        }
    }

    private static String convertType(String type) {
        switch (type.toUpperCase()) {
            case "BIT":
            case "TINYINT":
                type = "TINYINT";
                break;
            case "SMALLINT":
                type = "SMALLINT";
                break;
            case "INT":
            case "MEDIUMINT":
            case "INTEGER":
            case "YEAR":
            case "INT2":
            case "INT4":
            case "INT8":
                type = "INT";
                break;
            case "NUMERIC":
            case "NUMBER":
            case "BIGINT":
                type = "BIGINT";
                break;
            case "REAL":
            case "FLOAT":
            case "FLOAT2":
            case "FLOAT4":
            case "FLOAT8":
                type = "FLOAT";
                break;
            case "DOUBLE":
            case "BINARY_DOUBLE":
                type = "DOUBLE";
                break;
            case "DECIMAL":
                type = "DECIMAL";
                break;
            case "STRING":
            case "VARCHAR":
            case "VARCHAR2":
            case "CHAR":
            case "CHARACTER":
            case "NCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "LONGVARCHAR":
            case "LONGNVARCHAR":
            case "NVARCHAR":
            case "NVARCHAR2":
                type = "STRING";
                break;
            case "BINARY":
                type = "BINARY";
                break;
            case "BOOLEAN":
                type = "BOOLEAN";
                break;
            case "DATE":
                type = "DATE";
                break;
            case "TIMESTAMP":
                type = "TIMESTAMP";
                break;
            default:
                type = "STRING";
        }
        return type;
    }
}

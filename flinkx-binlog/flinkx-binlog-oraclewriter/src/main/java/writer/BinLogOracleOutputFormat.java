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

package writer;

import com.dtstack.flinkx.binlog.writerutil.*;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.mysql.format.MysqlOutputFormat;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author toutian    mvn clean package -Dmaven.test.skip=true
 */
public class BinLogOracleOutputFormat extends BaseRichOutputFormat {

    private static final String SP = "/";

    /**
     * hdfs高可用配置
     */
    protected Map<String, Object> hadoopConfig;

    protected String fileType;

    /**
     * 写入模式
     */
    protected String writeMode;

    /**
     * 压缩方式
     */
    protected String compress;

    protected String defaultFs;

    protected String delimiter;

    protected String charsetName = "UTF-8";

    protected Configuration conf;

    protected int rowGroupSize;

    protected long maxFileSize;

    /* ----------以上hdfs插件参数----------- */

    protected Map<String, TableInfo> tableInfos;
    protected   Map<String, List<Map<String, Object>>> tableMes;
    protected Map<String, String> distributeTableMapping;
    protected String partition;
    protected String partitionType;
    protected long bufferSize;
    protected String jdbcUrl;
    protected String username;
    protected String password;
    protected String tableBasePath;
    protected boolean autoCreateTable;
    protected String schema;

    private transient BinlogWriterUtil binlogWriterUtil;
    private transient TimePartitionFormat partitionFormat;

    private org.apache.flink.configuration.Configuration parameters;
    private int taskNumber;
    private int numTasks;

    private Map<String, TableInfo> tableCache;
    //private Map<String, BaseHdfsOutputFormat> outputFormats;

    private Map<String, FormatState> formatStateMap = new HashMap<>();

    private transient Gson gson;

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        this.parameters = parameters;

        partitionFormat = TimePartitionFormat.getInstance(partitionType);
        tableCache = new HashMap<>(16);
        //outputFormats = new HashMap<>(16);
        gson = GsonUtil.setTypeAdapter(new Gson());
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        if (null != formatState && null != formatState.getState()) {
            BinlogMysqlFormatState hiveFormatState = (BinlogMysqlFormatState)formatState.getState();
            formatStateMap.putAll(hiveFormatState.getFormatStateMap());
        }

        BinlogWriterDbUtil.ConnectionInfo connectionInfo = new BinlogWriterDbUtil.ConnectionInfo();
        connectionInfo.setJdbcUrl(jdbcUrl);
        connectionInfo.setUsername(username);
        connectionInfo.setPassword(password);
        connectionInfo.setHiveConf(hadoopConfig);

        binlogWriterUtil = new BinlogWriterUtil(connectionInfo);
        //primaryCreateTable();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        Map<String, FormatState> formatStateMap = flushOutputFormat();

        BinlogMysqlFormatState hiveFormatState = new BinlogMysqlFormatState(formatStateMap);
        formatState.setState(hiveFormatState);

        super.getFormatState();
        return formatState;
    }

    private Map<String, FormatState> flushOutputFormat() {
        //Map<String, FormatState> formatStateMap = new HashMap<>(outputFormats.size());
        //Iterator<Map.Entry<String, BaseHdfsOutputFormat>> entryIterator = outputFormats.entrySet().iterator();
        /*while (entryIterator.hasNext()) {
            Map.Entry<String, BaseHdfsOutputFormat> entry = entryIterator.next();
            FormatState formatState = entry.getValue().getFormatState();
            formatStateMap.put(entry.getValue().getFormatId(), formatState);

            if (partitionFormat.isTimeout(entry.getValue().getLastWriteTime())) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                } finally {
                    entryIterator.remove();
                }
            }
        }*/

        return formatStateMap;
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("HiveWriter");
    }

    
    /**
     * @author zhangchm
     * @date 2021-06-18 10:52
     * @param row
     * @return 
     * @explain说明:增量数据更新（从binlog、logminer、cdc中读取的数据）
    */
    @Override
    @SuppressWarnings("unchecked")
    public void writeRecord(Row row) throws IOException {
        boolean fromLogData = false;
        String tablePath;
        Map event = null;
        if (row.getField(0) instanceof Map) {
            event = (Map) row.getField(0);

            if (null != event && event.containsKey("message")) {
                Object tempObj = event.get("message");
                if (tempObj instanceof Map) {
                    event = (Map) tempObj;
                } else if (tempObj instanceof String) {
                    try {
                        event = MapUtil.jsonStrToObject((String) tempObj,Map.class);
                    }catch (IOException e){
                        // is not a json string
                        //tempObj 不是map类型 则event直接往下传递
                       // LOG.warn("bad json string:【{}】", tempObj);
                    }
                }
            }

            tablePath = PathConverterUtil.regaxByRules(event, tableBasePath, distributeTableMapping);
            fromLogData = true;
        } else {
            tablePath = tableBasePath;
        }

        try {
            dataToDatabase(event,row);//数据插入
        } catch (Exception e) {
            e.printStackTrace();
        }

        Pair<MysqlOutputFormat, TableInfo> formatPair;
        try {
            //formatPair = getHdfsOutputFormat(tablePath, event);
        } catch (Exception e) {
            throw new RuntimeException("get HDFSOutputFormat failed", e);
        }

        Row rowData = row;
        if (fromLogData) {
            //rowData = setChannelInformation(event, row.getField(1), formatPair.getSecond().getColumns());
        }

        try {
            //formatPair.getFirst().writeRecord(rowData);

            //row包含map嵌套的数据内容和channel， 而rowData是非常简单的纯数据，此处补上数据差额
            if (fromLogData && bytesWriteCounter != null) {
                bytesWriteCounter.add((long)row.toString().getBytes().length - rowData.toString().getBytes().length);
            }
        } catch (Exception e) {
            // 写入产生的脏数据已经由hdfsOutputFormat处理了，这里不用再处理了，只打印日志
            if (numWriteCounter.getLocalValue() % LOG_PRINT_INTERNAL == 0) {
                LOG.warn("write hdfs exception:", e);
            }
        }
    }



    /**
     * @author zhangchm
     * @date 2021-06-18 13:48
     * @param event
     * @param row
     * @return
     * @explain说明:增量数据写入目标数据库
    */
    private void dataToDatabase(Map event,Row row) throws Exception {
        //数据入库,获取数据库连接
        Connection connection = BinlogWriterDbUtil.CONNECTION;
        if (connection==null){
            try {
                connection= DbUtil.getConnection(jdbcUrl,username,password);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        String table=event.get("table").toString();//数据表数据源
        Map<String, List<Map<String, Object>>> map=tableMes;//目标数据表
        if (StringUtils.isEmpty(table)){
            throw new Exception("表不存在");
        }
        String targetTable="";
        String mainKey="";
        for (Map.Entry<String, List<Map<String, Object>>> entry : map.entrySet()) {//遍历表
            List<Map<String, Object>> tableColumn=entry.getValue();
            targetTable=entry.getKey();
            String sql="";
            for (int i = 0; i <tableColumn.size() ; i++) {
                Map<String, Object> columnMap=tableColumn.get(i);
                if(columnMap.containsKey("table")){
                    if(columnMap.get("table").equals(table)){//对比当前表的数据源表是否对应成功
                        mainKey=columnMap.get("key")==null ? "" : String.valueOf(columnMap.get("key"));
                        //如果取到目标表那么在当前位置组装sql
                        sql=BinlogWriterDbUtil.getDataSql(event.get("type").toString(),event,tableColumn,targetTable,mainKey);
                        break;
                    }
                }
            }
            PreparedStatement prep=connection.prepareStatement(sql);
            prep.execute();
            System.out.println("key = " + entry.getKey() + ", value = " + entry.getValue());
            }
        }




    @Override
    public void closeInternal() throws IOException {
        //closeOutputFormats();
    }

    static class BinlogMysqlFormatState implements Serializable {
        private Map<String, FormatState> formatStateMap;

        public BinlogMysqlFormatState(Map<String, FormatState> formatStateMap) {
            this.formatStateMap = formatStateMap;
        }

        public Map<String, FormatState> getFormatStateMap() {
            return formatStateMap;
        }

        public void setFormatStateMap(Map<String, FormatState> formatStateMap) {
            this.formatStateMap = formatStateMap;
        }
    }
}

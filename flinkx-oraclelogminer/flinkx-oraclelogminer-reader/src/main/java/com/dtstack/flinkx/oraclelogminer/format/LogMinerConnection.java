/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.oraclelogminer.format;

import com.dtstack.flinkx.oraclelogminer.entity.QueueData;
import com.dtstack.flinkx.oraclelogminer.entity.RecordLog;
import com.dtstack.flinkx.oraclelogminer.util.SqlUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RetryUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * @author jiangbo
 * @date 2020/3/27
 */
public class LogMinerConnection {
    public static Logger LOG = LoggerFactory.getLogger(LogMinerConnection.class);

    public static final String KEY_PRIVILEGE = "PRIVILEGE";
    public static final String KEY_GRANTED_ROLE = "GRANTED_ROLE";

    public static final String DBA_ROLE = "DBA";
    public static final String LOG_TYPE_ARCHIVED = "ARCHIVED";
    public static final String EXECUTE_CATALOG_ROLE = "EXECUTE_CATALOG_ROLE";

    public static final int ORACLE_11_VERSION = 11;
    public int oracleVersion;
    //oracle10????????????????????????????????????GBK
    public boolean isGBK = false;
    boolean isOracle10;

    public static final List<String> PRIVILEGES_NEEDED = Arrays.asList(
            "CREATE SESSION",
            "LOGMINING",
            "SELECT ANY TRANSACTION",
            "SELECT ANY DICTIONARY");

    public static final List<String> ORACLE_11_PRIVILEGES_NEEDED = Arrays.asList(
            "CREATE SESSION",
            "SELECT ANY TRANSACTION",
            "SELECT ANY DICTIONARY");

    public static final int RETRY_TIMES = 3;

    public static final int SLEEP_TIME = 2000;

    public final static String KEY_SEG_OWNER = "SEG_OWNER";
    public final static String KEY_TABLE_NAME = "TABLE_NAME";
    public final static String KEY_OPERATION = "OPERATION";
    public final static String KEY_OPERATION_CODE = "OPERATION_CODE";
    public final static String KEY_TIMESTAMP = "TIMESTAMP";
    public final static String KEY_SQL_REDO = "SQL_REDO";
    public final static String KEY_SQL_UNDO = "SQL_UNDO";
    public final static String KEY_CSF = "CSF";
    public final static String KEY_SCN = "SCN";
    public final static String KEY_CURRENT_SCN = "CURRENT_SCN";
    public final static String KEY_FIRST_CHANGE = "FIRST_CHANGE#";
    public final static String KEY_ROLLBACK = "ROLLBACK";
    public final static String KEY_ROW_ID = "ROW_ID";
    public final static String KEY_XID_SQN = "XIDSQN";


    private LogMinerConfig logMinerConfig;

    private Connection connection;

    private CallableStatement logMinerStartStmt;

    private PreparedStatement logMinerSelectStmt;

    private ResultSet logMinerData;

    private QueueData result;

    private List<LogFile> addedLogFiles = new ArrayList<>();

    private long lastQueryTime;

    private static final long QUERY_LOG_INTERVAL = 10000;

    private boolean logMinerStarted = false;

    /** ???delete?????????rollback?????????????????????insert?????????connection*/
    private LogMinerConnection queryDataForRollbackConnection;

    /**??????????????????  xidsqn(??????id)+rowId,scn**/
    Cache<String, LinkedHashMap<BigDecimal, RecordLog>> insertRecordCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(20, TimeUnit.MINUTES)
            .build();
    /**
     * minScn ???maxScn ?????????????????????????????????????????????????????????logminer???
     */

    /** ?????????logminer????????????????????? ?????????firstChange **/
    private BigDecimal minScn = null;
    /** ?????????maxScn??????????????????????????????logminer??????rac????????????????????????logminer???????????????????????????nextChange?????? **/
    private BigDecimal maxScn = null;


    public LogMinerConnection(LogMinerConfig logMinerConfig) {
        this.logMinerConfig = logMinerConfig;
    }

    public void connect() {
        try {
            ClassUtil.forName(logMinerConfig.getDriverName(), getClass().getClassLoader());

            connection = RetryUtil.executeWithRetry(() -> DriverManager.getConnection(logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), logMinerConfig.getPassword()), RETRY_TIMES, SLEEP_TIME, false);

            oracleVersion = connection.getMetaData().getDatabaseMajorVersion();
            isOracle10 = oracleVersion == 10;

            //??????session????????? NLS_DATE_FORMAT ?????? "YYYY-MM-DD HH24:MI:SS"??????????????????????????? redolog?????? TO_DATE('18-APR-21', 'DD-MON-RR')
            boolean isAlterDateFormat = false;
            try {
                try (PreparedStatement preparedStatement = connection.prepareStatement(SqlUtil.SQL_QUERY_NLS_DATE_FORMAT)) {
                    try (ResultSet resultSet = preparedStatement.executeQuery(SqlUtil.SQL_QUERY_NLS_DATE_FORMAT)) {
                        while (resultSet.next()) {
                            String nlsDateFormat = resultSet.getString(1);
                            isAlterDateFormat = !nlsDateFormat.equalsIgnoreCase("YYYY-MM-DD HH24:MI:SS");
                            LOG.info("nlsDateFormat {}, isAlterDateFormat {}", nlsDateFormat, isAlterDateFormat);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.info("query nlsDateFormat failed ,exception is {}", ExceptionUtil.getErrorMessage(e));
            }

            if (isAlterDateFormat) {
                try (PreparedStatement preparedStatement = connection.prepareStatement(SqlUtil.SQL_ALTER_DATE_FORMAT)) {
                    preparedStatement.execute();
                }
                try (PreparedStatement preparedStatement = connection.prepareStatement(SqlUtil.NLS_TIMESTAMP_FORMAT)) {
                    preparedStatement.execute();
                }
            }

            LOG.info("get connection successfully, url:{}, username:{}, Oracle version???{}", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), oracleVersion);
        } catch (Exception e) {
            String message = String.format("get connection failed???url:[%s], username:[%s], e:%s", logMinerConfig.getJdbcUrl(), logMinerConfig.getUsername(), ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            //???????????? ????????????connection,??????connection ??? session???????????? ??????????????????
            closeResources(null, null, connection);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * ??????LogMiner??????
     */
    public void disConnect() {
        //??????????????????????????????LogMiner?????????????????????????????????
        addedLogFiles.clear();

        if (null != logMinerStartStmt && logMinerStarted) {
            try {
                logMinerStartStmt.execute(SqlUtil.SQL_STOP_LOG_MINER);
            } catch (SQLException e) {
                LOG.warn("close logMiner failed, e = {}", ExceptionUtil.getErrorMessage(e));
            }
            logMinerStarted = false;
        }

        closeStmt(logMinerStartStmt);
        closeResources(logMinerData, logMinerSelectStmt, connection);

        //queryDataForRollbackConnection ?????????????????????
        if(Objects.nonNull(queryDataForRollbackConnection)){
            queryDataForRollbackConnection.disConnect();
        }
    }

    /**
     * ??????LogMiner
     *
     * @param startScn
     */
    public void startOrUpdateLogMiner(BigDecimal startScn) {
        String startSql = null;
        try {
            // ?????????????????????????????????????????????????????????????????????????????????????????? QUERY_LOG_INTERVAL
            if (lastQueryTime > 0) {
                long time = System.currentTimeMillis() - lastQueryTime;
                if (time < QUERY_LOG_INTERVAL) {
                    try {
                        Thread.sleep(QUERY_LOG_INTERVAL - time);
                    } catch (InterruptedException e) {
                        LOG.warn("", e);
                    }
                }
            }
            lastQueryTime = System.currentTimeMillis();

            if (logMinerConfig.getSupportAutoAddLog()) {
                startSql = isOracle10 ? SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG_10 : SqlUtil.SQL_START_LOG_MINER_AUTO_ADD_LOG;
            } else {
                //???????????????,??????maxScn??????
                if (null == maxScn) {
                    maxScn = startScn;
                }

                List<LogFile> newLogFiles = queryLogFiles(maxScn);
                if (addedLogFiles.equals(newLogFiles)) {
                    return;
                } else {
                    addedLogFiles = newLogFiles;
                    startSql = SqlUtil.SQL_START_LOGMINER;
                }
            }

            resetLogminerStmt(startSql);
            if (logMinerConfig.getSupportAutoAddLog()) {
                logMinerStartStmt.setBigDecimal(1, startScn);
            } else {
                logMinerStartStmt.setBigDecimal(1, minScn);
                logMinerStartStmt.setBigDecimal(2, maxScn);
            }

            logMinerStartStmt.execute();
            logMinerStarted = true;
            //??????????????????logminer??????????????????
            List<LogFile> logFiles = queryAddedLogFiles();
            LOG.info("start logMiner successfully, startScn:{}, minScn:{}, maxScn:{}, Log group changed, new log group = {}", startScn, minScn, maxScn, GsonUtil.GSON.toJson(logFiles));
        } catch (SQLException e) {
            String message = String.format("start logMiner failed, offset:[%s], sql:[%s], e: %s", startScn, startSql, ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    public void startOrUpdateLogminer(BigDecimal startScn, BigDecimal endScn) throws SQLException {
        resetLogminerStmt(SqlUtil.SQL_START_LOGMINER);

        logMinerStartStmt.setBigDecimal(1, startScn);
        logMinerStartStmt.setBigDecimal(2, endScn);
        logMinerStartStmt.execute();
        logMinerStarted = true;

    }

    /**
     * ???LogMiner??????????????????
     *
     * @param startScn
     * @param logMinerSelectSql
     */
    public void queryData(BigDecimal startScn, String logMinerSelectSql) {
        try {
            logMinerSelectStmt = connection.prepareStatement(logMinerSelectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setBigDecimal(1, startScn);
            logMinerSelectStmt.setBigDecimal(2, maxScn);
            logMinerData = logMinerSelectStmt.executeQuery();

            LOG.debug("query Log miner data, offset:{}", startScn);
        } catch (SQLException e) {
            String message = String.format("query logMiner data failed, sql:[%s], e: %s", logMinerSelectSql, ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    /**
     * ??????rollback????????? ???????????????dml??????
     * @param recordLog
     * @param sql
     */
    public void queryDataForDeleteRollback(RecordLog recordLog, String sql) {
        try {
            closeStmt();
            logMinerSelectStmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            configStatement(logMinerSelectStmt);

            logMinerSelectStmt.setFetchSize(logMinerConfig.getFetchSize());
            logMinerSelectStmt.setBigDecimal(1, recordLog.getScn());
            logMinerSelectStmt.setString(2, recordLog.getRowId());
            logMinerSelectStmt.setString(3, recordLog.getXidSqn());
            logMinerSelectStmt.setString(4, recordLog.getTableName());
            logMinerSelectStmt.setInt(5, 0);
            logMinerSelectStmt.setInt(6, 1);
            logMinerSelectStmt.setString(7, recordLog.getRowId());
            logMinerSelectStmt.setString(8, recordLog.getXidSqn());
            logMinerSelectStmt.setBigDecimal(9, recordLog.getScn());

            logMinerData = logMinerSelectStmt.executeQuery();

        } catch (SQLException e) {
            String message = String.format("queryDataForRollback failed, sql:[%s], recordLog:[%s] e: %s", sql, recordLog,ExceptionUtil.getErrorMessage(e));
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    public BigDecimal getStartScn(BigDecimal startScn) {
        // ??????????????????0?????????????????????????????????????????????????????????????????????
        if (null != startScn && startScn.compareTo(BigDecimal.ZERO) != 0) {
            startScn = getLogFileStartPositionByScn(startScn);
            return startScn;
        }

        // ???????????????0?????????????????????????????????
        if (ReadPosition.ALL.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // ??????????????????scn
            startScn = getMinScn();
        } else if (ReadPosition.CURRENT.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            startScn = getCurrentScn();
        } else if (ReadPosition.TIME.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // ????????????????????????????????????????????????????????????????????????
            if (logMinerConfig.getStartTime() == 0) {
                throw new IllegalArgumentException("[startTime] must not be null or empty when readMode is [time]");
            }

            startScn = getLogFileStartPositionByTime(logMinerConfig.getStartTime());
        } else if (ReadPosition.SCN.name().equalsIgnoreCase(logMinerConfig.getReadPosition())) {
            // ???????????????scn???????????????????????????????????????
            if (StringUtils.isEmpty(logMinerConfig.getStartScn())) {
                throw new IllegalArgumentException("[startSCN] must not be null or empty when readMode is [scn]");
            }

            startScn = new BigDecimal(logMinerConfig.getStartScn());
        } else {
            throw new IllegalArgumentException("unsupported readMode : " + logMinerConfig.getReadPosition());
        }

        return startScn;
    }

    /**
     * oracle????????????????????????????????????????????????????????? "FIRST_CHANGE" ??? "NEXT_CHANGE" ????????????,
     * ????????????????????????scn?????????????????????????????????????????????????????? "FIRST_CHANGE"?????????????????? "FIRST_CHANGE" ????????????,
     * ???[FIRST_CHANGE,scn] ?????????????????????????????????
     * <p>
     * ???????????????
     * v$archived_log ???????????????????????????????????????
     * v$log ????????????????????????????????????
     */
    private BigDecimal getLogFileStartPositionByScn(BigDecimal scn) {
        BigDecimal logFileFirstChange = null;
        PreparedStatement lastLogFileStmt = null;
        ResultSet lastLogFileResultSet = null;

        try {
            lastLogFileStmt = connection.prepareCall(isOracle10 ? SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN_10 : SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_SCN);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setBigDecimal(1, scn);
            lastLogFileStmt.setBigDecimal(2, scn);
            lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while (lastLogFileResultSet.next()) {
                logFileFirstChange = lastLogFileResultSet.getBigDecimal(KEY_FIRST_CHANGE);
            }

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("??????scn:[{}]??????????????????????????????????????????", scn, e);
            throw new RuntimeException(e);
        } finally {
            closeResources(lastLogFileResultSet, lastLogFileStmt, null);
        }
    }

    private BigDecimal getMinScn() {
        BigDecimal minScn = null;
        PreparedStatement minScnStmt = null;
        ResultSet minScnResultSet = null;

        try {
            minScnStmt = connection.prepareCall(SqlUtil.SQL_GET_LOG_FILE_START_POSITION);
            configStatement(minScnStmt);

            minScnResultSet = minScnStmt.executeQuery();
            while (minScnResultSet.next()) {
                minScn = minScnResultSet.getBigDecimal(KEY_FIRST_CHANGE);
            }

            return minScn;
        } catch (SQLException e) {
            LOG.error("??????????????????????????????????????????", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(minScnResultSet, minScnStmt, null);
        }
    }

    private BigDecimal getCurrentScn() {
        BigDecimal currentScn = null;
        CallableStatement currentScnStmt = null;
        ResultSet currentScnResultSet = null;

        try {
            currentScnStmt = connection.prepareCall(SqlUtil.SQL_GET_CURRENT_SCN);
            configStatement(currentScnStmt);

            currentScnResultSet = currentScnStmt.executeQuery();
            while (currentScnResultSet.next()) {
                currentScn = currentScnResultSet.getBigDecimal(KEY_CURRENT_SCN);
            }

            return currentScn;
        } catch (SQLException e) {
            LOG.error("???????????????SCN??????:", e);
            throw new RuntimeException(e);
        } finally {
            closeResources(currentScnResultSet, currentScnStmt, null);
        }
    }

    private BigDecimal getLogFileStartPositionByTime(Long time) {
        BigDecimal logFileFirstChange = null;

        PreparedStatement lastLogFileStmt = null;
        ResultSet lastLogFileResultSet = null;

        try {
            String timeStr = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");

            lastLogFileStmt = connection.prepareCall(isOracle10 ? SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME_10 : SqlUtil.SQL_GET_LOG_FILE_START_POSITION_BY_TIME);
            configStatement(lastLogFileStmt);

            lastLogFileStmt.setString(1, timeStr);
            lastLogFileStmt.setString(2, timeStr);

            if (!isOracle10) {
                //oracle10??????????????????
                lastLogFileStmt.setString(3, timeStr);
            }
            lastLogFileResultSet = lastLogFileStmt.executeQuery();
            while (lastLogFileResultSet.next()) {
                logFileFirstChange = lastLogFileResultSet.getBigDecimal(KEY_FIRST_CHANGE);
            }

            return logFileFirstChange;
        } catch (SQLException e) {
            LOG.error("????????????:[{}]??????????????????????????????????????????", time, e);
            throw new RuntimeException(e);
        } finally {
            closeResources(lastLogFileResultSet, lastLogFileStmt, null);
        }
    }

    /**
     * ???????????????????????????
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        closeStmt(stmt);

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

    /**
     * ??????scn?????????????????????????????????
     *
     * @param scn
     * @return
     * @throws SQLException
     */
    private List<LogFile> queryLogFiles(BigDecimal scn) throws SQLException {
        List<LogFile> logFileLists = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet rs = null;
        BigDecimal onlineNextChange = null;
        try {
            statement = connection.prepareStatement(isOracle10 ? SqlUtil.SQL_QUERY_LOG_FILE_10 : SqlUtil.SQL_QUERY_LOG_FILE);
            statement.setBigDecimal(1, scn);
            statement.setBigDecimal(2, scn);
            rs = statement.executeQuery();
            while (rs.next()) {
                LogFile logFile = new LogFile();
                logFile.setFileName(rs.getString("name"));
                logFile.setFirstChange(rs.getBigDecimal("first_change#"));
                logFile.setNextChange(rs.getBigDecimal("next_change#"));
                logFile.setThread(rs.getLong("thread#"));
                logFile.setBytes(rs.getLong("BYTES"));
                logFileLists.add(logFile);
                //?????????nextChange?????????online???nextChange
                if (onlineNextChange == null || onlineNextChange.compareTo(logFile.getNextChange()) < 0) {
                    onlineNextChange = logFile.getNextChange();
                }
            }
        } finally {
            closeResources(rs, statement, null);
        }
        //??????????????????????????????????????? ????????????
        List<LogFile> logFiles = new ArrayList<>(32);

        Map<Long, List<LogFile>> map = logFileLists.stream().collect(Collectors.groupingBy(LogFile::getThread));

        //????????????thread?????????????????????
        map.forEach((k, v) -> {
           map.put(k,v.stream().sorted(Comparator.comparing(LogFile::getFirstChange)).collect(Collectors.toList()));
        });

        BigDecimal tempMinFirstScn = null;
        BigDecimal tempMinNextScn = scn;

        long fileSize = 0L;
        Collection<List<LogFile>> values = map.values();

        while (fileSize < logMinerConfig.getMaxLogFileSize()) {
            List<LogFile> tempList = new ArrayList<>(8);
            for (List<LogFile> logFileList : values) {
                for (LogFile logFile1 : logFileList) {
                    if (!logFiles.contains(logFile1)) {
                        //??????thread?????????????????????????????????
                        tempList.add(logFile1);
                        break;
                    }
                }
            }
            //???????????? ??????????????????????????????????????? ????????????
            if (CollectionUtils.isEmpty(tempList)) {
                break;
            }
            //???????????????firstSCN????????????nextSCN
            BigDecimal minFirstScn = tempList.stream().sorted(Comparator.comparing(LogFile::getFirstChange)).collect(Collectors.toList()).get(0).getFirstChange();
            BigDecimal minNextScn = tempList.stream().sorted(Comparator.comparing(LogFile::getNextChange)).collect(Collectors.toList()).get(0).getNextChange();

            for (LogFile logFile1 : tempList) {
                if (logFile1.getFirstChange().compareTo(minNextScn) < 0) {
                    logFiles.add(logFile1);
                    fileSize += logFile1.getBytes();
                }
            }

            if (tempMinFirstScn == null) {
                tempMinFirstScn = minFirstScn;
            }
            tempMinNextScn = minNextScn;
        }
        //???????????????nextScn??????onlineNextChange???????????????????????????????????????
        if (tempMinNextScn.equals(onlineNextChange)) {
            //??????logminer?????????????????????????????????online???????????????????????????endScn????????????SCN
            tempMinNextScn = getCurrentScn();
            logFiles = logFileLists;
        }
        maxScn = tempMinNextScn;
        minScn = tempMinFirstScn;
        lastQueryTime = System.currentTimeMillis();
        return logFiles;
    }

    /**
     * ??????logminer?????????????????????
     */
    private List<LogFile> queryAddedLogFiles() throws SQLException {
        List<LogFile> logFileLists = new ArrayList<>();
        try(PreparedStatement statement = connection.prepareStatement(SqlUtil.SQL_QUERY_ADDED_LOG)) {
           try(ResultSet rs = statement.executeQuery()) {
               while (rs.next()) {
                   LogFile logFile = new LogFile();
                   logFile.setFileName(rs.getString("filename"));
                   logFile.setFirstChange(rs.getBigDecimal("low_scn"));
                   logFile.setNextChange(rs.getBigDecimal("next_scn"));
                   logFile.setThread(rs.getLong("thread_id"));
                   logFile.setBytes(rs.getLong("filesize"));
                   logFile.setStatus(rs.getInt("status"));
                   logFile.setType(rs.getString("type"));
                   logFileLists.add(logFile);
               }
           }
        }
        return logFileLists;
    }

    public boolean hasNext() throws SQLException, UnsupportedEncodingException, DecoderException {
        if (null == logMinerData || logMinerData.isClosed()) {
            return false;
        }

        String sqlLog;
        while (logMinerData.next()) {
            String sql = logMinerData.getString(KEY_SQL_REDO);
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            StringBuilder sqlRedo = new StringBuilder(sql);
            StringBuilder sqlUndo = new StringBuilder(Objects.nonNull(logMinerData.getString(KEY_SQL_UNDO)) ? logMinerData.getString(KEY_SQL_UNDO) : "");
            if (SqlUtil.isCreateTemporaryTableSql(sqlRedo.toString())) {
                continue;
            }
            BigDecimal scn = logMinerData.getBigDecimal(KEY_SCN);
            String operation = logMinerData.getString(KEY_OPERATION);
            int operationCode = logMinerData.getInt(KEY_OPERATION_CODE);

            boolean hasMultiSql = false;

            String xidSqn = logMinerData.getString(KEY_XID_SQN);
            String rowId = logMinerData.getString(KEY_ROW_ID);


            if (logMinerData.getBoolean(KEY_ROLLBACK)) {
                StringBuilder undoLog = new StringBuilder(1024);

                //??????????????????rollback?????????insert??????
                RecordLog recordLog = queryUndoLogFromCache(xidSqn + rowId, scn);
                if(Objects.nonNull(recordLog)){
                    undoLog.append(recordLog.getSqlUndo());
                    hasMultiSql = recordLog.getHasMultiSql();
                }

                if (undoLog.length() == 0) {
                    //??????insert?????????????????? ?????? ???rollback?????????????????????????????? ?????????????????????????????????
                    recordLog = recursionQueryDataForRollback(new RecordLog(scn, "", "", xidSqn, rowId, operationCode, false, logMinerData.getString(KEY_TABLE_NAME)));
                    if (Objects.nonNull(recordLog)) {
                        undoLog.append(recordLog.getSqlUndo());
                        hasMultiSql = recordLog.getHasMultiSql();
                    }
                }

                if(undoLog.length() == 0){
                    //????????????????????????insert?????? ??????delete where rowid=xxx ????????????redoLog
                    LOG.warn("has not found undoLog for scn {}",scn);
                }else{
                    sqlRedo = undoLog;
                }
                LOG.debug("there is a rollback sql,scn is {},rowId is {},xisSqn is {}", scn, rowId, xidSqn);
            } else {
                // ???CSF???????????????sql??????????????????????????????sql??????4000 ???????????????????????????
                boolean isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
                //??????????????????SQL
                hasMultiSql = isSqlNotEnd;

                while (isSqlNotEnd) {
                    logMinerData.next();
                    //redoLog ??????????????????????????????  ??????sqlUndo????????????????????????redolog??????null
                    String sqlRedoValue = logMinerData.getString(KEY_SQL_REDO);
                    if (Objects.nonNull(sqlRedoValue)) {
                        sqlRedo.append(sqlRedoValue);
                    }

                    String sqlUndoValue = logMinerData.getString(KEY_SQL_UNDO);
                    if (Objects.nonNull(sqlUndoValue)) {
                        sqlUndo.append(sqlUndoValue);
                    }
                    isSqlNotEnd = logMinerData.getBoolean(KEY_CSF);
                }
            }

            //oracle10??????????????????????????????4000???LogMiner??????????????????????????????SQL????????????
            if(hasMultiSql && isOracle10 && isGBK){
                String redo = sqlRedo.toString();
                String hexStr = new String(Hex.encodeHex(redo.getBytes("GBK")));
                boolean hasChange = false;
                if (operationCode ==1 && hexStr.contains("3f2c")) {
                    LOG.info("current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}", scn, redo, hexStr);
                    hasChange = true;
                    hexStr = hexStr.replace("3f2c", "272c");
                }
                if (operationCode != 1) {
                    if (hexStr.contains("3f20616e64")) {
                        LOG.info("current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}", scn, redo, hexStr);
                        hasChange = true;
                        //update set "" = '' and "" = '' where "" = '' and "" = '' where???????????????????????????
                        //delete from where "" = '' and "" = '' where???????????????????????????
                        //???????and -> '??????and
                        hexStr = hexStr.replace("3f20616e64", "2720616e64");
                    }

                    if (hexStr.contains("3f207768657265")) {
                        LOG.info("current scn is: {},\noriginal redo sql is: {},\nhex redo string is: {}", scn, redo, hexStr);
                        hasChange = true;
                        // ? where ?????? ' where
                        hexStr = hexStr.replace("3f207768657265", "27207768657265");
                    }
                }

                if(hasChange){
                    sqlLog = new String(Hex.decodeHex(hexStr.toCharArray()), "GBK");
                    LOG.info("final redo sql is: {}", sqlLog);
                }else{
                    sqlLog = sqlRedo.toString();
                }
            } else {
                sqlLog = sqlRedo.toString();
            }

            String schema = logMinerData.getString(KEY_SEG_OWNER);
            String tableName = logMinerData.getString(KEY_TABLE_NAME);
            Timestamp timestamp = logMinerData.getTimestamp(KEY_TIMESTAMP);

            Map<String, Object> data = new HashMap<>();
            data.put("schema", schema);
            data.put("tableName", tableName);
            data.put("operation", operation);
            data.put("sqlLog", sqlLog);
            data.put("opTime", timestamp);

            result = new QueueData(scn, data);

            //???????????????????????????
            putCache(new RecordLog(scn, sqlUndo.toString(), sqlLog, xidSqn, rowId, operationCode, hasMultiSql, tableName));
            return true;
        }

        return false;
    }

    //????????????????????????
    public boolean isValid() {
        try {
            return connection != null && connection.isValid(2000);
        } catch (SQLException e) {
            return false;
        }
    }

    public void checkPrivileges() {
        try (Statement statement = connection.createStatement()) {

            queryDataBaseEncoding();

            List<String> roles = getUserRoles(statement);
            if (roles.contains(DBA_ROLE)) {
                return;
            }

            if (!roles.contains(EXECUTE_CATALOG_ROLE)) {
                throw new IllegalArgumentException("???DBA????????????????????????[EXECUTE_CATALOG_ROLE]??????,?????????sql?????????GRANT EXECUTE_CATALOG_ROLE TO USERNAME");
            }

            if (containsNeededPrivileges(statement)) {
                return;
            }

            String message;
            if (ORACLE_11_VERSION <= oracleVersion) {
                message = "????????????????????????sql?????????GRANT CREATE SESSION, EXECUTE_CATALOG_ROLE, SELECT ANY TRANSACTION, FLASHBACK ANY TABLE, SELECT ANY TABLE, LOCK ANY TABLE, SELECT ANY DICTIONARY TO USER_ROLE;";
            } else {
                message = "????????????????????????sql?????????GRANT LOGMINING, CREATE SESSION, SELECT ANY TRANSACTION ,SELECT ANY DICTIONARY TO USER_ROLE;";
            }

            throw new IllegalArgumentException(message);
        } catch (SQLException e) {
            throw new RuntimeException("??????????????????", e);
        }
    }


    private boolean containsNeededPrivileges(Statement statement) {
        try (ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_PRIVILEGES)) {
            List<String> privileges = new ArrayList<>();
            while (rs.next()) {
                String privilege = rs.getString(KEY_PRIVILEGE);
                if (StringUtils.isNotEmpty(privilege)) {
                    privileges.add(privilege.toUpperCase());
                }
            }

            int privilegeCount = 0;
            List<String> privilegeList;
            if (oracleVersion <= ORACLE_11_VERSION) {
                privilegeList = ORACLE_11_PRIVILEGES_NEEDED;
            } else {
                privilegeList = PRIVILEGES_NEEDED;
            }
            for (String privilege : privilegeList) {
                if (privileges.contains(privilege)) {
                    privilegeCount++;
                }
            }

            return privilegeCount == privilegeList.size();
        } catch (SQLException e) {
            throw new RuntimeException("????????????????????????", e);
        }
    }

    private List<String> getUserRoles(Statement statement) {
        try (ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ROLES)) {
            List<String> roles = new ArrayList<>();
            while (rs.next()) {
                String role = rs.getString(KEY_GRANTED_ROLE);
                if (StringUtils.isNotEmpty(role)) {
                    roles.add(role.toUpperCase());
                }
            }

            return roles;
        } catch (SQLException e) {
            throw new RuntimeException("????????????????????????", e);
        }
    }

    /**
     * ??????Oracle10????????????????????????
     */
    private void queryDataBaseEncoding() {
        if (isOracle10) {
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(SqlUtil.SQL_QUERY_ENCODING)) {
                rs.next();
                String encoding = rs.getString(1);
                LOG.info("current oracle encoding is {}", encoding);
                isGBK = encoding.contains("GBK");
            } catch (SQLException e) {
                throw new RuntimeException("????????????????????????", e);
            }
        }
    }

    private void configStatement(java.sql.Statement statement) throws SQLException {
        if (logMinerConfig.getQueryTimeout() != null) {
            statement.setQueryTimeout(logMinerConfig.getQueryTimeout().intValue());
        }
    }

    public QueueData next() {
        return result;
    }


    /**
     * ??????logMinerSelectStmt
     */
    public void closeStmt() {
        try {
            if (logMinerSelectStmt != null && !logMinerSelectStmt.isClosed()) {
                logMinerSelectStmt.close();
            }
        } catch (SQLException e) {
            LOG.warn("Close logMinerSelectStmt error", e);
        }
        logMinerSelectStmt = null;
    }

    /**
     * ??????Statement
     */
    private void closeStmt(Statement statement) {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn("Close statement error", e);
        }
    }


    private void putCache(RecordLog recordLog) {
        //??????????????????insert???DML??????
        if (recordLog.getOperationCode() != 1) {
            return;
        }
        Map<BigDecimal, RecordLog> bigDecimalListMap = insertRecordCache.getIfPresent(recordLog.getXidSqn() + recordLog.getRowId());
        if (Objects.isNull(bigDecimalListMap)) {
            LinkedHashMap<BigDecimal, RecordLog> data = new LinkedHashMap<>(32);
            insertRecordCache.put(recordLog.getXidSqn() + recordLog.getRowId(), data);
            bigDecimalListMap = data;
        }
          bigDecimalListMap.put(recordLog.getScn(), recordLog);

    }


    /**
     * ????????????insert???????????????rollback???????????????DML??????
     * ??????????????? ?????????????????????????????????
     * @param key xidSqn+rowid
     * @param scn scn of rollback
     * @return insert Log
     */
    public RecordLog queryUndoLogFromCache(String key, BigDecimal scn) {
        LinkedHashMap<BigDecimal, RecordLog> recordMap = insertRecordCache.getIfPresent(key);
        if (MapUtils.isEmpty(recordMap)) {
            return null;
        }
        //??????scn????????? ??????scn????????? ??????????????????DML??????
        RecordLog recordLog = recordMap.get(scn);
        if (Objects.isNull(recordLog)) {
            //??????scn?????????DML???????????? ????????????????????????rowId?????????????????????
            Iterator<Map.Entry<BigDecimal, RecordLog>> iterator = recordMap.entrySet().iterator();
            Map.Entry<BigDecimal, RecordLog> tail = null;
            while (iterator.hasNext()) {
                tail = iterator.next();
            }
             if(Objects.nonNull(tail)){
                 recordLog = tail.getValue();
                 recordMap.remove(tail.getKey());
             }
        } else {
            recordMap.remove(scn);
        }
        LOG.info("query a insert sql for rollback in cache,rollback scn is {}",scn);
        return recordLog;
    }


    /**
     * ???????????? delete???rollback?????????insert??????
     * @param rollbackRecord  rollback??????
     * @return insert??????
     */
    public RecordLog recursionQueryDataForRollback(RecordLog rollbackRecord) throws SQLException, UnsupportedEncodingException, DecoderException {
        if (Objects.isNull(queryDataForRollbackConnection)) {
            queryDataForRollbackConnection = new LogMinerConnection(logMinerConfig);
        }

        if (Objects.isNull(queryDataForRollbackConnection.connection) || queryDataForRollbackConnection.connection.isClosed()){
            LOG.info("queryDataForRollbackConnection start connect");
            queryDataForRollbackConnection.connect();
        }

        //???????????????????????????????????????????????????scn  ???????????????scn???????????????
        List<LogFile> logFiles = queryAddedLogFiles().stream().filter(i->i.getStatus() != 4 && i.getType().equalsIgnoreCase(LOG_TYPE_ARCHIVED)).collect(Collectors.toList());

        //????????????????????????4000???scn
        BigDecimal step = new BigDecimal(4000);
        if (CollectionUtils.isNotEmpty(logFiles)) {
            //nextChange-firstChange ??????????????????????????????scn?????????*2 ???????????????scn??????2?????????
            step = logFiles.get(0).getNextChange().subtract(logFiles.get(0).getFirstChange()).multiply(new BigDecimal(2));
        }

        BigDecimal startScn = rollbackRecord.getScn().subtract(step);
        BigDecimal endScn = rollbackRecord.getScn();

        for (int i = 0; i < 10; i++) {
            queryDataForRollbackConnection.startOrUpdateLogminer(startScn, endScn);
            queryDataForRollbackConnection.queryDataForDeleteRollback(rollbackRecord, SqlUtil.queryDataForRollback);
            //while???????????????????????? ???????????????????????????
            while (queryDataForRollbackConnection.hasNext()){}
            //???????????????
            RecordLog insertLog = queryDataForRollbackConnection.queryUndoLogFromCache(rollbackRecord.getXidSqn() + rollbackRecord.getRowId(), rollbackRecord.getScn());
            if (Objects.nonNull(insertLog)) {
                return insertLog;
            }
            endScn = startScn;
            startScn = startScn.subtract(step);
        }
        return null;
    }

    /**
     * ?????? ??????logminer???statement
     * @param startSql
     * @throws SQLException
     */
    public void resetLogminerStmt(String startSql) throws SQLException {
        closeStmt(logMinerStartStmt);
        logMinerStartStmt = connection.prepareCall(startSql);
        configStatement(logMinerStartStmt);
    }

    public enum ReadPosition {
        ALL, CURRENT, TIME, SCN
    }
}

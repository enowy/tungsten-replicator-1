/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.applier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.Writer;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.FailurePolicy;
import com.continuent.tungsten.replicator.database.AdditionalTypes;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.datatypes.MySQLUnsignedNumeric;
import com.continuent.tungsten.replicator.datatypes.Numeric;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;
import com.continuent.tungsten.replicator.extractor.oracle.redo.RedoReaderManager;
import com.continuent.tungsten.replicator.plugin.PluginContext;

import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.CLOB;

/**
 * Applier for Oracle DBMS. This class handles Oracle-specific DBMS types and
 * implements JDBC batching, which is well supported by the Oracle JDBC driver.
 */
public class OracleApplier extends JdbcApplier
{
	
	// TODO Review
    // Thread that marks plogs obsolete
	// This is done asynchronously in order to avoid commit being delayed by VMRR
    private class ObsoletePlogThread extends Thread
    {
        public String path;
        public int retainedPlogs;

        /**
         * Sets the path value.
         * 
         * @param path The path to set.
         */
        public void setPath(String path)
        {
            this.path = path;
        }

        /**
         * Sets the retainedPlogs value.
         * 
         * @param retainedPlogs The retainedPlogs to set.
         */
        public void setRetainedPlogs(int retainedPlogs)
        {
            this.retainedPlogs = retainedPlogs;
        }

        public void run()
        {
            try
            {
                if(!vmrrMgr.isRunning())
                    vmrrMgr.start();
                    
                vmrrMgr.registerExtractor();

                int plog = findMostRecentPlogFile();
                if(plog > 0)
                    vmrrMgr.reportLastObsoletePlog(plog);
                else
                    logger.warn("No plog files found");
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
            catch (ReplicatorException e)
            {
                e.printStackTrace();
            }
        }

        // TODO Review
        // Based on the code from the extractor.
        // This gets the first in time file sharing the same max plog number
        private int findMostRecentPlogFile()
                throws FileNotFoundException
        {
            if (path == null)
            {
                throw new NullPointerException(
                        "plogDirectory was not set in properties file");
            }

            final String plog = ".plog.";
            File dir = new File(path);
            File[] matchingFiles = dir.listFiles(new FilenameFilter()
            {
                public boolean accept(File dir, String name)
                {
                    String file = name.toLowerCase();
                    return file.contains(plog) && !file.endsWith(".gz");
                }
            });
            Arrays.sort(matchingFiles);

            long minTime = 0;
            File minTimeFile = null;
            int plogSequence = 0;

            for (int i = matchingFiles.length - 1; i >= 0; i--)
            {
                String name = matchingFiles[i].getName().toLowerCase();
                String timeStr;

                int index = name.indexOf(plog);
                if (minTimeFile == null)
                {
                    minTimeFile = matchingFiles[i];
                    timeStr = name.substring(index + plog.length());
                    minTime = Long.parseLong(timeStr);
                    plogSequence = Integer
                            .parseInt(name.substring(0, index));
                }
                else
                {
                    if (plogSequence == Integer
                            .parseInt(name.substring(0, index)))
                    {
                        // Same sequence number... check that it is
                        // older than currently selected file
                        timeStr = name.substring(index + plog.length());
                        long time = Long.parseLong(timeStr);
                        if (time < minTime)
                        {
                            minTimeFile = matchingFiles[i];
                            timeStr = name
                                    .substring(index + plog.length());
                            minTime = time;
                        }
                    }
                    else
                        // This is a normal plog number. Subtract the
                        // retained plogs so
                        // we don't delete prematurely and return.
                        return Math.max(plogSequence - retainedPlogs,
                                0);
                }
            }
            return -1;
        }
    }

    private static Logger     logger              = Logger
            .getLogger(OracleApplier.class);

    // TODO Review
    // This is derived from other property (replicator.extractor.dbms exists and
    // is equal to
    // com.continuent.tungsten.replicator.extractor.oracle.redo.PlogExtractor).
    private boolean           hasPlogs = false;
    private RedoReaderManager vmrrMgr;

    private String            replicateApplyName;
    
    // TODO Review
    // Used to compute when the obsolete thread should run
    private long              lastObsoleteTaskRunTime = 0;

    // TODO Review
    // For now, these use extractor defined parameters but the first one
    // could/should probably be an applier setting as we may want to keep less
    // plogs on the slave side than we do on the master side
    private int               retainedPlogs           = 20;
    private String            plogPath;

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException
    {
        super.configure(context);

        String property = context.getReplicatorProperties().getProperty("replicator.extractor.dbms", "");
        hasPlogs = property.length() > 0 && property.equals("com.continuent.tungsten.replicator.extractor.oracle.redo.PlogExtractor");
        
        if (hasPlogs)
        {
            logger.info("Plog extractor is configured for this applier.");
            if (this.replicateApplyName == null)
            {
                replicateApplyName = context.getServiceName();
            }
            this.retainedPlogs = context.getReplicatorProperties().getInt("replicator.extractor.dbms.retainedPlogs");
            this.plogPath = context.getReplicatorProperties().get("replicator.extractor.dbms.plogDirectory");
        }
        else
        {
            logger.info("Plog extractor is not configured for this applier. Skipping.");
        }
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        super.prepare(context);
        
        if (hasPlogs)
        {
            // Locate the vmrr control script and use this to configure the redo
            // reader manager.
            String clusterHome;
            try
            {
                clusterHome = ClusterConfiguration.getClusterHome();
            }
            catch (ConfigurationException e)
            {
                throw new ReplicatorException(
                        "Unable to locate cluster-home directory; ensure replicator is properly installed",
                        e);
            }

            String vmrrControlScriptName = "bin/vmrrd_" + context.getServiceName();
            String vmrrControlScript = new File(clusterHome, vmrrControlScriptName)
                    .getAbsolutePath();

            vmrrMgr = new RedoReaderManager();
            vmrrMgr.setVmrrControlScript(vmrrControlScript);
            vmrrMgr.setReplicateApplyName(replicateApplyName);
            vmrrMgr.initialize();
        }
    }

    /**
     * Applies one or more sets of row changes with JDBC batching.
     */
    @Override
    protected void applyOneRowChangePrepared(OneRowChange oneRowChange, String sourceDbmsType)
            throws ReplicatorException
    {
        // This method only handles event batching, which has specific Oracle
        // semantics.
        if (!optimizeRowEvents)
        {
            super.applyOneRowChangePrepared(oneRowChange, sourceDbmsType);
            return;
        }

        getColumnInformation(oneRowChange);

        String stmt = null;

        ArrayList<OneRowChange.ColumnSpec> key = oneRowChange.getKeySpec();
        ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                .getColumnSpec();

        try
        {
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                    .getKeyValues();
            ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                    .getColumnValues();

            int row = 0;
            for (row = 0; row < columnValues.size()
                    || row < keyValues.size(); row++)
            {
                if (row == 0 || needNewSQLStatement(row, keyValues, key,
                        columnValues, columns))
                {
                    ArrayList<OneRowChange.ColumnVal> keyValuesOfThisRow = null;
                    if (keyValues.size() > 0)
                        keyValuesOfThisRow = keyValues.get(row);

                    // Construct separate SQL for every row, because there might
                    // be NULLs in keys in which case SQL is different
                    // (TREP-276).
                    ArrayList<OneRowChange.ColumnVal> colValuesOfThisRow = null;
                    if (columnValues.size() > 0)
                        colValuesOfThisRow = columnValues.get(row);

                    stmt = constructStatement(oneRowChange.getAction(),
                            oneRowChange.getSchemaName(),
                            oneRowChange.getTableName(), columns, key,
                            keyValuesOfThisRow, colValuesOfThisRow).toString();

                    // Check to see if we need to flush the pending batch.
                    if (pendingSqlStatement != null)
                    {
                        if (!pendingSqlStatement.equals(stmt))
                        {
                            // Flush if the statement is different.
                            if (logger.isDebugEnabled())
                            {
                                logger.debug(
                                        "Flushing batch due to different SQL: old=["
                                                + pendingSqlStatement
                                                + "] new=[" + stmt + "]");
                            }
                            executePendingBatch();
                            prepareNewBatch(stmt);
                        }
                        else if (pendingRowChanges.size() >= maxRowBatchSize)
                        {
                            // Flush if the number of row changes exceeds
                            // maximum.
                            if (logger.isDebugEnabled())
                            {
                                logger.debug(
                                        "Flushing batch due to maximum row changes: pendingRowChanges="
                                                + pendingRowChanges.size()
                                                + " maxRowBatchSize="
                                                + this.maxRowBatchSize);
                            }
                            executePendingBatch();
                            prepareNewBatch(stmt);
                        }
                        else
                        {
                            // Otherwise we can just append the changes.
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Appending next change to batch");
                            }
                        }
                    }
                    else
                    {
                        // We do not have a pending batch, so just prepare a new
                        // one.
                        prepareNewBatch(stmt);
                    }
                }

                int bindLoc = 1; /* Start binding at index 1 */

                /* bind column values */
                if (columnValues.size() > 0)
                {
                    bindLoc = bindColumnValues(pendingPreparedStatement,
                            columnValues.get(row), bindLoc, columns, false, sourceDbmsType);
                }
                /* bind key values */
                // Do not try to bind key values, which have been added to make
                // heterogeneous cluster slave to work as part of Issue 1079,
                // for INSERTs.
                if (oneRowChange.getAction() != RowChangeData.ActionType.INSERT
                        && keyValues.size() > 0)
                {
                    bindLoc = bindColumnValues(pendingPreparedStatement,
                            keyValues.get(row), bindLoc, key, true, sourceDbmsType);
                }

                // Now add the batch.
                this.addToPendingBatch(oneRowChange, row);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Added change to batch: action="
                            + oneRowChange.getAction() + " row=" + row);
                }
            }
        }
        catch (SQLException e)
        {
            // This is a garden-variety exception. Show at least the statement.
            ApplierException applierException = new ApplierException(
                    "Batch update failed: statement=" + stmt, e);
            throw applierException;
        }
    }

    /** Prepare for a new SQL batch. */
    @Override
    protected void prepareNewBatch(String stmt) throws SQLException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Preparing new JDBC batch");
        }
        this.pendingSqlStatement = stmt;
        this.pendingPreparedStatement = conn.prepareStatement(stmt);
        this.pendingRowChanges = new LinkedList<RowReference>();
    }

    /**
     * Add a new batch to the current pending prepared statement.
     * 
     * @throws ReplicatorException
     */
    @Override
    protected void addToPendingBatch(OneRowChange oneRowChange, int row)
            throws ReplicatorException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding change row to pending batch");
        }
        try
        {
            pendingPreparedStatement.addBatch();
            RowReference rowRef = new RowReference();
            rowRef.rowChanges = oneRowChange;
            rowRef.row = row;
            pendingRowChanges.add(rowRef);
        }
        catch (SQLException e)
        {
            ReplicatorException replicatorException = new ReplicatorException(
                    "Unable to add pending row change to JDBC batch");
            replicatorException.setExtraData(logFailedRowChangeSQL(
                    pendingSqlStatement, oneRowChange, row));
            throw replicatorException;
        }
    }

    /**
     * Execute the current batch.
     */
    @Override
    protected void executePendingBatch() throws ReplicatorException
    {
        if (this.pendingPreparedStatement == null)
        {
            // This is permitted to make client usage simple.
            return;
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Executing current pending batch: statement="
                    + this.pendingSqlStatement + " change count="
                    + this.pendingRowChanges.size());
        }

        int[] checkCounts;
        int updateCount = 0;
        try
        {
            checkCounts = pendingPreparedStatement.executeBatch();

            for (int i = 0; i < checkCounts.length; i++)
            {
                int updatedRows = checkCounts[i];
                if (logger.isDebugEnabled())
                {
                    logger.debug("Checking update status: update=" + i
                            + " updatedRows=" + updatedRows);
                }
                if (updatedRows == Statement.SUCCESS_NO_INFO)
                {
                    // Worked but there is no row count.
                    continue;
                }
                else if (updatedRows == Statement.EXECUTE_FAILED)
                {
                    // Batch update failed without an exception.
                    RowReference rowRef = this.pendingRowChanges.get(i);
                    ApplierException applierException = new ApplierException(
                            "Batch update failed without exception");
                    applierException.setExtraData(
                            logFailedRowChangeSQL(this.pendingSqlStatement,
                                    rowRef.rowChanges, rowRef.row));
                    throw applierException;
                }
                else if (updatedRows == 0)
                {
                    RowReference rowRef = this.pendingRowChanges.get(i);
                    if (runtime
                            .getApplierFailurePolicyOn0RowUpdates() == FailurePolicy.WARN)
                    {
                        logger.warn(
                                "UPDATE or DELETE statement did not process any row\n"
                                        + logFailedRowChangeSQL(
                                                this.pendingSqlStatement,
                                                rowRef.rowChanges, rowRef.row));
                    }
                    else
                        if (runtime
                                .getApplierFailurePolicyOn0RowUpdates() == FailurePolicy.STOP)
                    {
                        ReplicatorException replicatorException = new ReplicatorException(
                                "SQL statement did not process any row");
                        replicatorException.setExtraData(
                                logFailedRowChangeSQL(this.pendingSqlStatement,
                                        rowRef.rowChanges, rowRef.row));
                        throw replicatorException;
                    }
                    else
                    {
                        // else IGNORE
                    }
                }
                else
                {
                    updateCount += updatedRows;
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("Applied event (update count " + updateCount
                            + "): " + this.pendingSqlStatement.toString());
                }
            }
        }
        catch (BatchUpdateException e)
        {
            // This exception occurred during a batch. Get the list of rows
            // that updated successfully before the failure.
            ApplierException applierException;
            int updateCounts[] = e.getUpdateCounts();
            int index = updateCounts.length + 1;

            // If safe to do so, show the row *after* the last failed row.
            // Otherwise just show the statement.
            if (index < this.pendingRowChanges.size())
            {
                applierException = new ApplierException(e);
                ;
                RowReference rowRef = this.pendingRowChanges.get(index);
                applierException.setExtraData(
                        logFailedRowChangeSQL(this.pendingSqlStatement,
                                rowRef.rowChanges, rowRef.row));
            }
            else
            {
                applierException = new ApplierException(
                        "Batch update failed: statement="
                                + this.pendingSqlStatement,
                        e);
            }
            throw applierException;
        }
        catch (SQLException e)
        {
            // This is a garden-variety exception. Show at least the statement.
            ApplierException applierException = new ApplierException(
                    "Batch update failed: statement="
                            + this.pendingSqlStatement,
                    e);
            throw applierException;
        }
        finally
        {
            // Clear the pending batch now that we have applied it.
            releasePendingBatch();
        }
    }

    /** Release pending batch JDBC resources. */
    @Override
    protected void releasePendingBatch()
    {
        if (this.pendingPreparedStatement != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Releasing pending batch variables");
            }
            try
            {
                // Close the statement.
                this.pendingPreparedStatement.close();
            }
            catch (SQLException e)
            {
            }
            finally
            {
                // Null out everything to release resources.
                this.pendingPreparedStatement = null;
                this.pendingRowChanges = null;
                this.pendingSqlStatement = null;
            }
        }
    }

    private CLOB getCLOB(String xmlData) throws SQLException
    {
        CLOB tempClob = null;
        Connection dbConn = conn.getConnection();
        try
        {
            // If the temporary CLOB has not yet been created, create new
            tempClob = CLOB.createTemporary(dbConn, true,
                    CLOB.DURATION_SESSION);

            // Open the temporary CLOB in readwrite mode to enable writing
            tempClob.open(CLOB.MODE_READWRITE);
            // Get the output stream to write
            // Writer tempClobWriter = tempClob.getCharacterOutputStream();
            Writer tempClobWriter = tempClob.setCharacterStream(0);
            // Write the data into the temporary CLOB
            tempClobWriter.write(xmlData);

            // Flush and close the stream
            tempClobWriter.flush();
            tempClobWriter.close();

            // Close the temporary CLOB
            tempClob.close();
        }
        catch (SQLException sqlexp)
        {
            tempClob.freeTemporary();
            sqlexp.printStackTrace();
        }
        catch (Exception exp)
        {
            tempClob.freeTemporary();
            exp.printStackTrace();
        }
        return tempClob;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#setObject(java.sql.PreparedStatement,
     *      int, com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal,
     *      com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec)
     */
    @Override
    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            ColumnVal value, ColumnSpec columnSpec, String sourceDbmsType) throws SQLException
    {
        int type = columnSpec.getType();
        try
        {
            if (value.getValue() == null)
                prepStatement.setObject(bindLoc, null);
            /*
             * prepStatement.setNull(bindLoc, type); else if (type ==
             * Types.FLOAT) ((OraclePreparedStatement)
             * prepStatement).setBinaryFloat( bindLoc, ((Float)
             * value.getValue()).floatValue()); else if (type == Types.DOUBLE)
             * ((OraclePreparedStatement) prepStatement).setBinaryDouble(
             * bindLoc, ((Double) value.getValue()).doubleValue());
             */
            else if (type == AdditionalTypes.XML)
            {
                CLOB clob = getCLOB((String) (value.getValue()));
                ((OraclePreparedStatement) prepStatement).setObject(bindLoc,
                        clob);
            }
            else if (type == Types.DATE
                    && (value.getValue() instanceof java.sql.Timestamp))
            { // Issue 704 - unsuccessful DATETIME to DATE conversion
                Timestamp ts = (Timestamp) value.getValue();
                ((OraclePreparedStatement) prepStatement).setTimestamp(bindLoc,
                        ts, Calendar.getInstance(TimeZone.getTimeZone("GMT")));
            }
            else if (type == Types.DATE
                    && (value.getValue() instanceof java.lang.Long))
            { // TENT-311 - no conversion is needed if the underlying value is
              // Date.
                Timestamp ts = new Timestamp((Long) (value.getValue()));
                ((OraclePreparedStatement) prepStatement).setObject(bindLoc,
                        ts);
            }
            else if (type == Types.BLOB || (type == Types.NULL
                    && value.getValue() instanceof SerialBlob))
            { // ______^______
              // Blob in the incoming event masked as NULL,
              // though this happens with a non-NULL value!
              // Case targeted with this: MySQL.TEXT -> Oracle.VARCHARx

                SerialBlob blob = (SerialBlob) value.getValue();

                if (columnSpec.isBlob() || (sourceDbmsType != null
                        && sourceDbmsType.equals(Database.ORACLE)))
                {
                    // Blob in the incoming event and in Oracle table.
                    // IMPORTANT: the bellow way only fixes INSERTs.
                    // Blobs in key lookups of DELETEs and UPDATEs is
                    // not supported.
                    // Warning : isBlob is set only if metadata is fetched from
                    // the underlying database, which is not the default anymore.
                	// Added a workaround to make it work in Oracle to Oracle case.
                    prepStatement.setBytes(bindLoc,
                            blob.getBytes(1, (int) blob.length()));
                    if (logger.isDebugEnabled())
                    {
                        logger.debug(
                                "BLOB support in Oracle is only for INSERT currently; key lookup during DELETE/UPDATE will result in an error");
                    }
                }
                else
                {
                    // Blob in the incoming event, but not in Oracle.
                    // Case targeted with this: MySQL.TEXT -> Oracle.VARCHARx
                    String toString = null;
                    if (blob != null)
                        toString = new String(
                                blob.getBytes(1, (int) blob.length()));
                    prepStatement.setString(bindLoc, toString);
                }
            }
            else if (type == Types.INTEGER)
            { // Issue 798 - MySQLExtractor extracts UNSIGNED numbers in a not
              // platform-indendent way.
                Object valToInsert = null;
                Numeric numeric = new Numeric(columnSpec, value);
                if (columnSpec.isUnsigned() && numeric.isNegative())
                {
                    // We assume that if column is unsigned - it's MySQL on the
                    // master side, as Oracle doesn't have UNSIGNED modifier.
                    valToInsert = MySQLUnsignedNumeric
                            .negativeToMeaningful(numeric);
                    prepStatement.setObject(bindLoc, valToInsert);
                }
                else
                    prepStatement.setObject(bindLoc, value.getValue());
            }
            else
                prepStatement.setObject(bindLoc, value.getValue());
        }
        catch (SQLException e)
        {
            logger.error("Binding column (bindLoc=" + bindLoc + ", type=" + type
                    + ") failed:");
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     * @throws SQLException 
     * @see com.continuent.tungsten.replicator.applier.JdbcApplier#commit()
     */
    @Override
    public void commitTransaction() throws SQLException
    {
        super.commitTransaction();
        
        if(hasPlogs)
        {
            reportObsoleteFiles();
            
            // TODO Review
            // Should we check health of VMRR on a different basis here ?
            // Under no load or if replicator is offline, VMRR health won't be
            // checked, but it should not be a big deal (except if the
            // replicator is offline for too long).
            // If we want to solve this last issue, then we should probably have a
            // wrapper project around VMRR that would do this basic health check.
        }
    }    
    
    @Override
    protected void rollbackTransaction() throws SQLException
    {
        if (!conn.isAutoCommit())
            super.rollbackTransaction();
        else
            logger.warn(
                    "Connection is set as auto-commit : impossible to rollback");
    }

    private void reportObsoleteFiles()
    {
    	// TODO Review
        // The time interval should be a setting with a minimum value to be
        // defined. For now, run the job every minute or so
        if (lastObsoleteTaskRunTime == 0 || System.currentTimeMillis()
                - lastObsoleteTaskRunTime > 60 * 1000)
        {
            // Time to mark obsolete files
            lastObsoleteTaskRunTime = System.currentTimeMillis();

            // TODO Review
            // Global vs local thread ?
            // For now, local thread. But might be interesting to have one global thread
            // that we run at time intervals
            ObsoletePlogThread obsoletePlogsReporter = new ObsoletePlogThread();
            obsoletePlogsReporter.setPath(plogPath);
            obsoletePlogsReporter.setRetainedPlogs(retainedPlogs);
            obsoletePlogsReporter.start();
        }
    }
}
/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc.
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
 * Initial developer(s): Vit Spinka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cache.RawByteCache;
import com.continuent.tungsten.common.config.cluster.ClusterConfiguration;
import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.datasource.SqlDataSource;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * We normally pass a DBMSEvent through the queue; this class just gives us
 * option to pass our internal error message, too.
 */
class PlogDBMSErrorMessage extends DBMSEvent
{
    private static final long serialVersionUID = 1300L;

    private String    message;
    private Throwable exception;

    public PlogDBMSErrorMessage(String message, Throwable exception)
    {
        this.message = message;
        this.exception = exception;
    }

    public Throwable getException()
    {
        return exception;
    }

    public String getMessage()
    {
        return message;
    }
}

/**
 * PlogExtractor class. This is the main entry point for this Replicate plugin.
 * It just spawns PlogReaderThread (so it can run in the background) and this
 * class then just passes the already parsed DBMSEvents to the caller.
 * <p/>
 * This extractor converts interleaved Oracle transactions to the fully
 * serialized Tungsten format. The conversion requires that large transactions
 * be buffered in order to await a final commit. The class buffers LCRs for
 * single transactions as Java objects up to a set number of objects and then
 * spills transactions into a byte vector cache once they exceed the limit.
 */
public class PlogExtractor implements RawExtractor
{
    private static Logger logger = Logger.getLogger(PlogExtractor.class);

    // Properties.
    private String plogDirectory = null;
    private int    retainedPlogs = 20;
    private String dataSource    = "extractor";

    private PlogReaderThread              readerThread;
    private ArrayBlockingQueue<DBMSEvent> queue;
    private String                        lastEventId;

    private boolean cancelled = false;

    private int    transactionFragSize     = 10000;
    private int    queueSize               = 100;
    private int    sleepSizeInMilliseconds = 1000;
    private int    healthCheckInterval     = 30;
    private String replicateApplyName;

    // Manager for vmrr process.
    private RedoReaderManager vmrrMgr;

    // Cache used for storing byte vectors. For now the cache does not
    // use object serialization to memory, hence the 0 defaults.
    private RawByteCache byteCache;
    private File         cacheDir;
    private long         cacheMaxTotalBytes  = 0;
    private long         cacheMaxObjectBytes = 0;
    private int          cacheMaxOpenFiles   = 50;

    // Number of LCRs to buffer as Java objects before using the cache.
    private int lcrBufferLimit = 10000;

    // Data source used to get DBMS connections.
    private SqlDataSource dataSourceImpl;

    // Flag so that we log information on first transaction to ensure good
    // diagnostics for restart issues.
    private boolean firstXactLatch = true;

    private ReplDBMSHeader lastEvent;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        if (this.replicateApplyName == null)
        {
            replicateApplyName = context.getServiceName();
        }
        if (cacheDir == null)
        {
            File home = ReplicatorRuntimeConf.locateReplicatorHomeDir();
            cacheDir = new File(home, "var/cache");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
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

        // Set up the byte cache for large objects.
        byteCache = new RawByteCache(cacheDir, cacheMaxTotalBytes,
                cacheMaxObjectBytes, cacheMaxOpenFiles);
        byteCache.prepare();

        // Note the number of LCRs that will be buffered as Java objects.
        logger.info(
                "Number of LCRs that will be buffered per transaction in Java before flushing to byte cache: "
                        + lcrBufferLimit);
        logger.info(
                "Raising lcrBufferLimit can speed extraction; lowering saves memory when there are concurrent large transactions");

        // Set up the data source so we can fetch current SCN, etc.
        // Locate our data source from which we are extracting.
        logger.info("Connecting to data source");
        dataSourceImpl = (SqlDataSource) context.getDataSource(dataSource);
        if (dataSourceImpl == null)
        {
            throw new ReplicatorException(
                    "Unable to locate data source: name=" + dataSource);
        }

        // Note number of retained plogs.
        logger.info("Maximum number of plogs to retain before compressing: "
                + retainedPlogs);

        // Set up the queue for reading.
        queue = new ArrayBlockingQueue<DBMSEvent>(queueSize);
        readerThread = new PlogReaderThread(context, queue, vmrrMgr, byteCache);
        readerThread.setName("plog-reader-task");
        readerThread.setRetainedPlogs(retainedPlogs);
        readerThread.setPlogDirectory(plogDirectory);
        readerThread.setSleepSizeInMilliseconds(sleepSizeInMilliseconds);
        readerThread.setHealthCheckInterval(healthCheckInterval);
        readerThread.setTransactionFragSize(transactionFragSize);
        readerThread.setLcrBufferLimit(lcrBufferLimit);
        readerThread.prepare();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        cancelled = true;

        // Stop the reader thread.
        if (readerThread != null)
        {
            readerThread.cancel();
            readerThread = null;
        }

        // Make a best effort to stop the redo reader process.
        if (vmrrMgr != null)
        {
        	// TODO Review
            // Not stopping the VMRR anymore -- it should be kept running even
            // after replicator goes offline
//            try
//            {
//                vmrrMgr.stop();
//            }
//            catch (ReplicatorException e)
//            {
//                logger.warn("Unable to stop vmrr process", e);
//            }
//            finally
//            {
//                vmrrMgr = null;
//            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        // Store for use when we start extraction.
        if (eventId != null)
        {
            lastEventId = eventId.trim();
            logger.info("Last event ID received: " + eventId);
        }
    }

    /**
     * Sets the plogDirectory value.
     * 
     * @param plogDirectory The plogDirectory to set.
     */
    public void setPlogDirectory(String plogDirectory)
    {
        logger.info("setPlogDirectory: set to [" + plogDirectory + "]");
        this.plogDirectory = plogDirectory;
    }

    /** Sets the number of plogs to retain uncompressed. */
    public void setRetainedPlogs(int retainedPlogs)
    {
        this.retainedPlogs = retainedPlogs;
    }

    /**
     * Sets the transactionFragSize value.
     * 
     * @param transactionFragSize The transactionFragSize to set.
     */
    public void setTransactionFragSize(int transactionFragSize)
    {
        logger.info(
                "setTransactionFragSize: set to [" + transactionFragSize + "]");
        this.transactionFragSize = transactionFragSize;
    }

    /**
     * Sets the queueSize value. This is the maximum number of rows than can be
     * handled by the queue (committed changes before plog reader pauses)
     * 
     * @param queueSize The queueSize to set.
     */
    public void setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
    }

    /**
     * Sets the sleepSizeInMilliseconds value - how long plog reader pauses if
     * no new data is in plog
     * 
     * @param sleepSizeInMilliseconds The sleepSizeInMilliseconds to set.
     */
    public void setSleepSizeInMilliseconds(int sleepSizeInMilliseconds)
    {
        this.sleepSizeInMilliseconds = sleepSizeInMilliseconds;
    }

    /**
     * Sets the number of sleep cycles to wait before doing a replicator health
     * check.
     */
    public void setHealthCheckInterval(int healthCheckInterval)
    {
        this.healthCheckInterval = healthCheckInterval;
    }

    /**
     * Sets the replicateApplyName value - name of this plog extractor
     * (necessary to set only if we use multiple plog extractors and we need to
     * distinguish them when setting last obsolete plog seq in mine)
     * 
     * @param replicateApplyName The name of this plog extractor to set.
     */
    public void setReplicateApplyName(String replicateApplyName)
    {
        this.replicateApplyName = replicateApplyName;
    }

    /** Directory that contains the object cache files. */
    public void setCacheDir(File cacheDir)
    {
        this.cacheDir = cacheDir;
    }

    /**
     * Total number of bytes in the object cache for in-memory objects. Any
     * write that would exceed this limit goes to storage.
     */
    public void setCacheMaxTotalBytes(long cacheMaxTotalBytes)
    {
        this.cacheMaxTotalBytes = cacheMaxTotalBytes;
    }

    /**
     * Maximum in-memory size for a single object. After this point, writes to
     * the object go to storage.
     */
    public void setCacheMaxObjectBytes(long cacheMaxObjectBytes)
    {
        this.cacheMaxObjectBytes = cacheMaxObjectBytes;
    }

    /**
     * Maximum number of cache files open for writing at any given time.
     */
    public void setCacheMaxOpenFiles(int cacheMaxOpenFiles)
    {
        this.cacheMaxOpenFiles = cacheMaxOpenFiles;
    }

    /**
     * Maximum number of LCRs to buffer as Java objects before spilling to byte
     * vector cache.
     */
    public void setLcrBufferLimit(int lcrBufferLimit)
    {
        this.lcrBufferLimit = lcrBufferLimit;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public DBMSEvent extract() throws ReplicatorException, InterruptedException
    {
        // TODO Review
        // Should NOW be dropped as it resets the VMRR, which should not be needed
        // Not changing that part for now.
        
        // Perform first time initialization.
        if (!readerThread.isAlive())
        {
            // See if we need to position the redo reader to read a different
            // SCN.
            if (this.lastEventId != null)
            {
                // Check to see the kind of last event ID and position redo
                if (lastEventId.startsWith("ora:"))
                {
                    logger.info(
                            "Starting redo reader after provisioning at SCN "
                                    + lastEventId.substring("ora:".length()));
                }
                else
                {
                    if (lastEventId.equalsIgnoreCase("NOW"))
                    {
                        // Position to current SCN, whatever that may be.
                        logger.info(
                                "Positioning redo reader to start from current SCN");
                        vmrrMgr.reset("NOW");
                    }
                    else if (lastEventId.matches("^[0-9]+$"))
                    {
                        // Position to an explicit SCN.
                        logger.info(
                                "Positioning redo reader to start from explicit SCN: "
                                        + lastEventId);
                        vmrrMgr.reset(lastEventId);
                    }
                    else
                    {
                        logger.info("Starting from previous eventId: "
                                + lastEventId);
                    }
                    // Set the redo reader thread restart position.
                    readerThread.setLastEventId(lastEventId);
                }
            }
            // TODO Review
            // This happens when switching --> if source change is detected, 
            // then the last applied event is used to determine coorect plog
            // restart position             
            else if (this.lastEvent != null) {
                readerThread.setLastEvent(lastEvent);
            }
            else
            {
                logger.info(
                        "Starting from current redo reader position, no eventId set.");
            }

            // Ensure the redo reader is started.
            // TODO Review
            // VMRR should be started at this point. This needs more work
            // related to the VMRR liveness checks and health checks.
            // This will be part of a second commit.
            if (!vmrrMgr.isRunning())
                vmrrMgr.start();

            // Start the thread.
            readerThread.start();
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Extractor start");
        }

        while (!cancelled)
        {
            DBMSEvent dbmsMsg = queue.take();
            if (logger.isDebugEnabled())
            {
                logger.debug("Extractor got DBMSEvent " + dbmsMsg.getEventId());
            }

            if (dbmsMsg instanceof DBMSEvent)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Extractor returns DBMSEvent "
                            + dbmsMsg.getEventId());
                }
                if (firstXactLatch)
                {
                    String eventId = dbmsMsg.getEventId();
                    logger.info(
                            "First event ID extracted from log after start: "
                                    + eventId);
                    firstXactLatch = false;
                }
                return dbmsMsg; /*
                                 * ReaderThread already fragmented as necessary,
                                 * just pass to caller
                                 */
            }
            else if (dbmsMsg instanceof PlogDBMSErrorMessage)
            {
                PlogDBMSErrorMessage errorMsg = (PlogDBMSErrorMessage) dbmsMsg;
                throw new ReplicatorException(errorMsg.getMessage(),
                        errorMsg.getException());
            }
        }

        logger.info("Extractor cancelled.");
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    @Override
    public DBMSEvent extract(String eventId)
            throws ReplicatorException, InterruptedException
    {
        setLastEventId(eventId);
        return extract();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    @Override
    public String getCurrentResourceEventId()
            throws ReplicatorException, InterruptedException
    {
        // select dbms_flashback.get_system_change_number current_scn from dual;
        // If we are not prepared, we cannot get status because we won't have a
        // data source available.
        if (dataSourceImpl == null)
            return "NONE";

        // Fetch SCN.
        Database conn = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            // Try to get a connection. If we cannot, return NONE as above.
            conn = dataSourceImpl.getUnmanagedConnection();
            if (conn == null)
                return "NONE";

            st = conn.createStatement();
            String query = "select dbms_flashback.get_system_change_number current_scn from dual";
            rs = st.executeQuery(query);
            if (!rs.next())
                throw new ReplicatorException(
                        "Oracle SCN query returned no rows: " + query);
            String scn = rs.getString(1);
            return scn;
        }
        catch (SQLException e)
        {
            logger.warn("Oracle SCN query failed: query=" + e.getMessage());
            throw new ReplicatorException(
                    "Oracle SCN query failed: " + e.getMessage(), e);
        }
        finally
        {
            cleanUpDatabaseResources(conn, st, rs);
        }
    }

    // Utility method to close result, statement, and connection objects.
    private void cleanUpDatabaseResources(Database conn, Statement st,
            ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (st != null)
        {
            try
            {
                st.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (conn != null)
        {
            conn.close();
        }
    }

    @Override
    public void setLastEvent(ReplDBMSHeader header)
    {
        this.lastEvent = header;
    }
}
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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
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
 */
public class PlogExtractor implements RawExtractor
{
    private static Logger logger = Logger.getLogger(PlogExtractor.class);

    private int    seq     = 1;
    private String lastSCN = null;

    private String plogDirectory = null;

    private PlogReaderThread              readerThread;
    private ArrayBlockingQueue<DBMSEvent> queue;

    private boolean cancelled = false;

    private int transactionFragSize = 10000;

    private int queueSize = 100;

    private int    sleepSizeInMilliseconds = 1000;
    private String replicateConsoleScript;
    private String replicateApplyName      = "TungstenApply";

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
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
        queue = new ArrayBlockingQueue<DBMSEvent>(queueSize);
        readerThread = new PlogReaderThread(context, queue, plogDirectory,
                sleepSizeInMilliseconds, transactionFragSize,
                replicateConsoleScript, replicateApplyName);

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
        if (readerThread != null)
            readerThread.cancel();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    @Override
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId != null)
        {
            logger.info("Starting from eventId " + eventId);
            readerThread.setLastEventId(eventId);
        }
        else
        {
            logger.info("Starting from fresh, no eventId set.");
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
     * Sets the replicateConsoleScript value - start-console.sh used to update
     * last obsole plog for mine
     * 
     * @param replicateConsoleScript The complete pah+name of the script.
     */
    public void setReplicateConsoleScript(String replicateConsoleScript)
    {
        this.replicateConsoleScript = replicateConsoleScript;
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

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    @Override
    public DBMSEvent extract() throws ReplicatorException, InterruptedException
    {
        if (!readerThread.isAlive())
            readerThread.start();

        logger.info("Extractor start");

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
        return null;
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
        return null;
    }

}
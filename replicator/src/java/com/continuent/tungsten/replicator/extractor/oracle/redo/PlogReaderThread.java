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
 * Contributor(s): Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cache.LargeObjectScanner;
import com.continuent.tungsten.common.cache.RawByteCache;
import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.datasource.SqlCommitSeqno;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Defines a replication event extractor, which reads events from plog file,
 * which abstracts Oracle REDO records. This is a thread that actually does the
 * extraction and passes it in queue to PlogExtractor.
 */
public class PlogReaderThread extends Thread
{
    private static Logger logger = Logger.getLogger(PlogReaderThread.class);

    private DataInputStream plogStream;
    private String          plogFilename;

    private int plogId;

    private BlockingQueue<DBMSEvent> queue;

    private PluginContext     context;
    private String            plogDirectory;
    private int               retainedPlogs       = 20;
    private int               sleepSizeInMilliseconds;
    private int               healthCheckInterval = 30;
    private int               transactionFragSize = 0;
    private String            tungstenSchema;
    private RedoReaderManager vmrrMgr;
    private RawByteCache      cache;
    private int               lcrBufferLimit;

    /*
     * dict cache enabled / disabled for current plog
     */
    private boolean             plogDictCacheEnabled;
    HashMap<Integer, String>    plogDictCacheObjectOwner = new HashMap<Integer, String>();
    HashMap<Integer, String>    plogDictCacheObjectName  = new HashMap<Integer, String>();
    HashMap<String, PlogLCRTag> plogDictCacheColumnName  = new HashMap<String, PlogLCRTag>();
    HashMap<String, PlogLCRTag> plogDictCacheColumnType  = new HashMap<String, PlogLCRTag>();

    private String lastProcessedEventId = null;

    // Cancelled flag must be volatile to ensure visibility of changes across
    // threads.
    private volatile boolean cancelled;

    private boolean         currentlyInIFILE;
    private DataInputStream plogStreamPaused;
    /*
     * parent plog if we are reading IFILE
     */
    private int             lastReportedObsolePlogSeq = 0;

    // Currently open transactions. Each transaction *must* also be released to
    // free resources from the cache.
    HashMap<String, PlogTransaction> openTransactions = new HashMap<String, PlogTransaction>();

    long   restartEventCommitSCN = 0;
    String restartEventXID       = "";
    String restartEventChangeSeq = "";
    long   restartEventStartSCN  = 0;
    int    restartEventStartPlog = 0;

    private IncludePlog currentIncludePlog;

    private ReplDBMSHeader lastEvent;

    /**
     * Defines information about so-called include plog, used by the LOAD
     * feature.
     */
    protected class IncludePlog
    {
        private String plogFilename;

        public IncludePlog(String plogFilename)
        {
            this.plogFilename = plogFilename;
        }
    }

    /**
     * Reads a "int" value from an InputStream. The value is converted to the
     * opposed endian system while reading.
     * 
     * @param input source InputStream
     * @return the value just read
     * @throws IOException in case of an I/O problem
     */
    private static int readSwappedInteger(final DataInputStream input)
            throws IOException
    {
        final int value1 = input.readByte();
        final int value2 = input.readByte();
        final int value3 = input.readByte();
        final int value4 = input.readByte();

        return ((value1 & 0xff) << 0) + ((value2 & 0xff) << 8)
                + ((value3 & 0xff) << 16) + ((value4 & 0xff) << 24);
    }

    /**
     * For start with no eventId, search a directory and find oldest (= lowest
     * sequence) plog.
     * 
     * TODO Review : This is now done in reverse order : find
     * the latest file with no gaps in the sequence of files and which is
     * greater than the last .gz file (if any).
     * 
     * @param path Directory to search
     * @return first sequence found. Integer.MAX_VALUE if no file present at
     *         all.
     * @throws ReplicatorException 
     */
    private static int findOldestPlogSequence(String path)
            throws FileNotFoundException, ReplicatorException
    {
        if (path == null)
        {
            throw new NullPointerException(
                    "plogDirectory was not set in properties file");
        }

        if (logger.isDebugEnabled())
            logger.debug("findOldestPlogSequence: " + path);

        File dir = new File(path);
        if (!dir.canRead())
        {
            throw new FileNotFoundException(path + " is not accessible");
        }
        File[] matchingFiles = dir.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                String file = name.toLowerCase();
                // TODO Review
                // Skip .gz files as this could break the extraction
                return file.contains(".plog."); // && !file.endsWith(".gz");
            }
        });

        Arrays.sort(matchingFiles);
        int minPlogSeq = Integer.MAX_VALUE;
        for (int i = matchingFiles.length - 1; i >= 0; i--)
        {
            File mf = matchingFiles[i];
            if (minPlogSeq == Integer.MAX_VALUE && mf.getName().endsWith(".gz"))
            {
                // This should not happen : the last file here is obsolete !
                throw new ReplicatorException(
                        "Unable to identify last plog. Last file "
                                + mf.getName()
                                + "is obsolete. Unable to start extraction. Try resetting !");
            }
            else
            {
                // We reached the first obsolete file, return sequence number
                // from previous normal plog file.
                if (mf.getName().endsWith(".gz"))
                {
                    break;
                }
                // Handling first normal plog.
                else if (minPlogSeq == Integer.MAX_VALUE)
                {
                    minPlogSeq = getPlogSequenceNumber(mf);
                }
                // Handling next plog files.
                else
                {
                    int currentPlogSeq = getPlogSequenceNumber(mf);
                    
                    if(currentPlogSeq == minPlogSeq)
                    {
                        // Just another plog with same sequence number (and
                        // different timestamp). Just check next file.
                        continue;
                    }
                    else if (currentPlogSeq == minPlogSeq - 1)
                    {
                        // No gap
                        minPlogSeq = currentPlogSeq;
                    }
                    else
                        // Gap : just return the previous file, for which there
                        // is no gap in the sequence
                        break;
                }
            }
        }
        return minPlogSeq;
    }

    private static int getPlogSequenceNumber(File mf)
    {
        int s = 0;
        String p = null;
        try
        {
            p = mf.getName();
            p = p.substring(0, p.indexOf(".plog."));
            s = Integer.parseInt(p);
        }
        catch (NumberFormatException e)
        {
            logger.warn(p + " is not really a time number");
        }
        catch (IndexOutOfBoundsException e)
        {
            logger.warn(p + " malformed, did not find .plog extension.");
        }
        return s;
    }

    /**
     * Find the most recent plog file for a given sequence. It expects the form
     * %seq%.plog.%timestamp% ; timestamp is an increasing integer (seconds
     * since epoch is the prime example).
     * 
     * @param path Directory to search
     * @param plogSequence Plog sequence to look for
     * @return Filename
     * @throws FileNotFoundException No file for that sequence found
     */
    private String findMostRecentPlogFile(String path, int plogSequence)
            throws FileNotFoundException
    {
        if (path == null)
        {
            throw new NullPointerException(
                    "plogDirectory was not set in properties file");
        }

        final String prefix = plogSequence + ".plog";
        File dir = new File(path);
        File[] matchingFiles = dir.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                String file = name.toLowerCase();
                // TODO Review
                // Skip .gz files as this breaks the extraction
                return file.toLowerCase().startsWith(prefix)
                        && !file.endsWith(".gz");
            }
        });
        long highestTime = 0;
        File highestTimeFile = null;
        for (File mf : matchingFiles)
        {
            String p = null;
            try
            {
                p = mf.getName().substring(prefix.length() + 1);
                long time = Long.parseLong(p);
                if (time > highestTime)
                {
                    highestTime = time;
                    highestTimeFile = mf;
                }
            }
            catch (NumberFormatException e)
            {
                logger.warn(p + " is not really a time number");
            }
            catch (IndexOutOfBoundsException e)
            {
                logger.warn(mf.getName() + " has no time suffix");
            }
        }
        if (highestTimeFile != null)
        {
            return highestTimeFile.getPath();
        }
        else
        {
            throw new FileNotFoundException(
                    "Cannot find latest file for sequence " + plogSequence);
        }
    }

    /**
     * Open plog file
     * 
     * @param filename Full path and filename
     * @throws FileNotFoundException
     */
    private void openFile(String filename) throws FileNotFoundException
    {
        plogStream = new DataInputStream(new BufferedInputStream(
                new FileInputStream(new File(filename))));
        logger.info("File " + filename + " opened.");
        if (logger.isDebugEnabled())
        {
            logger.debug("Last processed event ID=" + this.lastProcessedEventId);
        }
        logger.info("Open transactions=" + openTransactions.size());
        logger.info("Transaction cache statistics: " + cache.toString());
        if (logger.isDebugEnabled())
        {
            for (String xid : openTransactions.keySet())
            {
                PlogTransaction t = openTransactions.get(xid);
                StringBuffer sb = new StringBuffer();
                sb.append("TRANSACTION: ").append(t.toString());
                LargeObjectScanner<PlogLCR> scanner = null;
                try
                {
                    scanner = t.getLCRList().scanner();
                    while (scanner.hasNext())
                    {
                        PlogLCR lcr = scanner.next();
                        sb.append("\nLCR: ").append(lcr.toString());
                    }
                }
                catch (IOException e)
                {
                    sb.append("\nError: unable to scan PlogLCRs: "
                            + e.getMessage());
                }
                finally
                {
                    if (scanner != null)
                        scanner.close();
                }
                logger.debug(sb.toString());
            }
        }
    }

    /**
     * Close plog file. This operation is idempotent to allow convenient cleanup
     * at thread exit.
     */
    private void closeFile()
    {
        if (plogStream != null)
        {
            try
            {
                plogStream.close();
                logger.info("File " + plogFilename + " closed.");
            }
            catch (IOException e)
            {
                logger.warn("Unable to close file cleanly: " + plogFilename);
            }
            finally
            {
                plogStream = null;
            }
        }
    }

    /**
     * Read the plog file header, checking it's really a plog file and the
     * version is supported.
     * 
     * @throws IOException
     */
    private void readHeader() throws IOException
    {
        byte[] signatureBytes = new byte[8];
        for (int i = 0; i < 8; i++)
            signatureBytes[i] = plogStream.readByte();
        // the expected value is 7-bit ASCII, so no charset magic is needed here
        String stringifiedHeader = new String(signatureBytes, "US-ASCII");
        if (!stringifiedHeader.equals("PLOG\n \r "))
        {
            throw new IOException("File header does not denote a PLOG file.");
        }
        int major_file_version = readSwappedInteger(plogStream);
        int minor_file_version = readSwappedInteger(plogStream);
        if (major_file_version != 1 || minor_file_version != 1)
        {
            throw new IOException(
                    "File header indicates version " + major_file_version + "."
                            + minor_file_version + "; but it should be 1.1");
        }
        logger.info("Header and file version OK");
        return; // OK
    }

    /**
     * Read one LCR from the plog, split the data into plog tags. Do some basic
     * parsing like transaction id, LCR id etc. If plog dict caching is enabled,
     * maintain the dictionary here (so it is done in proper order, doing on
     * commit is too late)
     * 
     * @throws IOException
     */
    private PlogLCR readRawLCR() throws IOException
    {
        PlogLCR rawLCR = new PlogLCR();
        int objectId = 0;
        boolean objectHadMetadata = false;
        try
        {
            plogStream.mark(1000000);
            rawLCR.length = readSwappedInteger(plogStream);
            rawLCR.type = readSwappedInteger(plogStream);
            rawLCR.subtype = readSwappedInteger(plogStream);
            rawLCR.rawTags = new LinkedList<PlogLCRTag>();
            int lengthSoFar = 3; /* 3 for length, type, subtype */
            while (lengthSoFar < rawLCR.length)
            {
                PlogLCRTag tag = readRawLCRTag();
                lengthSoFar += tag.length;

                if (tag.id == PlogLCRTag.TAG_XID)
                {
                    rawLCR.XID = tag.valueString();
                }
                else if (tag.id == PlogLCRTag.TAG_LCR_ID)
                {
                    rawLCR.LCRid = tag.valueLong() + 1000000000L/* 1e9 */
                            * plogId;
                }
                else if (tag.id == PlogLCRTag.TAG_PLOGSEQ)
                {
                    plogId = tag.valueInt();
                }
                else if (tag.id == PlogLCRTag.TAG_SAVEPOINT_ID)
                {
                    rawLCR.LCRSavepointId = tag.valueLong();
                }
                else if (tag.id == PlogLCRTag.TAG_SCN)
                {
                    rawLCR.SCN = tag.valueLong();
                }
                else if (tag.id == PlogLCRTag.TAG_DTIME)
                {
                    rawLCR.timestamp = tag.valueDtime();
                }
                else if (tag.id == PlogLCRTag.TAG_OBJ_OWNER)
                {
                    rawLCR.tableOwner = tag.valueString();
                    objectHadMetadata = true;
                }
                else if (tag.id == PlogLCRTag.TAG_OBJ_NAME)
                {
                    rawLCR.tableName = tag.valueString();
                    objectHadMetadata = true;
                }
                else if (tag.id == PlogLCRTag.TAG_OBJ_ID)
                {
                    objectId = tag.valueInt();
                    rawLCR.tableId = objectId;
                }
                else
                /* will be processed later */
                {
                    rawLCR.rawTags.add(tag);
                }
            }
        }
        catch (IOException e)
        {
            plogStream.reset();
            throw (e);
        }

        if (rawLCR.type == PlogLCR.ETYPE_LCR_DATA && this.plogDictCacheEnabled)
        {
            /*
             * we assume: 1. we process the plog by SCNs (and as it was written)
             * -> a missing info is always obtainable from last present cache
             * entry 2. we never reread old LCRs -> we never need any older
             * entry -> we don't care about storing older entries and we don't
             * even need to check SCNs
             */
            if (objectHadMetadata)
            {
                plogDictCacheObjectOwner.put(objectId, rawLCR.tableOwner);
                plogDictCacheObjectName.put(objectId, rawLCR.tableName);
                /* add object to dict cache */
            }
            else if (!objectHadMetadata)
            {
                /*
                 * set values from cache; no need to actually add tags, nobody
                 * needs them
                 */
                rawLCR.tableOwner = plogDictCacheObjectOwner.get(objectId);
                rawLCR.tableName = plogDictCacheObjectName.get(objectId);
            }

            int columnId = -1;
            boolean lastColumnHadMetadata = false;
            PlogLCRTag lastColumnName = null;
            PlogLCRTag lastColumnType = null;

            LinkedList<PlogLCRTag> newRawTags = new LinkedList<PlogLCRTag>();
            String objColId = null;
            for (PlogLCRTag tag : rawLCR.rawTags)
            {
                switch (tag.id)
                {
                    case PlogLCRTag.TAG_COL_ID :
                        if (objColId != null && !lastColumnHadMetadata)
                        {
                            /*
                             * add tags from dict cache (only if the previous
                             * column had no data at all)
                             */
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Get from ColumnName: [" + objColId
                                        + "]=" + plogDictCacheColumnName
                                                .get(objColId).valueString());
                                logger.debug("Get from ColumnType: [" + objColId
                                        + "]=" + plogDictCacheColumnType
                                                .get(objColId).valueString());
                            }
                            newRawTags
                                    .add(plogDictCacheColumnName.get(objColId));
                            newRawTags
                                    .add(plogDictCacheColumnType.get(objColId));
                        }
                        columnId = tag.valueInt();
                        objColId = "" + objectId + "." + columnId;
                        lastColumnName = null;
                        lastColumnType = null;
                        lastColumnHadMetadata = false;
                        break;
                    case PlogLCRTag.TAG_COL_NAME :
                        lastColumnName = tag;
                        lastColumnHadMetadata = true;
                        plogDictCacheColumnName.put(objColId, lastColumnName);
                        break;
                    case PlogLCRTag.TAG_COL_TYPE :
                        lastColumnType = tag;
                        lastColumnHadMetadata = true;
                        plogDictCacheColumnType.put(objColId, lastColumnType);
                        break;
                    case PlogLCRTag.TAG_PREIMAGE :
                    case PlogLCRTag.TAG_POSTIMAGE :
                    case PlogLCRTag.TAG_KEYIMAGE :
                    case PlogLCRTag.TAG_LOBDATA :
                        /*
                         * place the looked-up values before these tags - this
                         * is the correct place for this to happen
                         */
                        if (objColId != null && !lastColumnHadMetadata)
                        {
                            /* add tags from dict cache */
                            newRawTags
                                    .add(plogDictCacheColumnName.get(objColId));
                            newRawTags
                                    .add(plogDictCacheColumnType.get(objColId));
                            lastColumnHadMetadata = true;
                        }
                        break;
                    default :
                        /*
                         * other tags are of no interest for us here, we handle
                         * column data only here
                         */
                }
                newRawTags.add(tag);
            }

            if (objColId != null && !lastColumnHadMetadata)
            {
                /*
                 * add tags from dict cache (only for last column and if it had
                 * no data tag at all)
                 */
                newRawTags.add(plogDictCacheColumnName.get(objColId));
                newRawTags.add(plogDictCacheColumnType.get(objColId));
            }

            rawLCR.rawTags = newRawTags;
        }

        return rawLCR;
    }

    /**
     * Read control header info. We especially want to know if plog dicrionary
     * is enabled.
     * 
     * @throws IOException
     */
    private void readControlHeader(PlogLCR LCR) throws IOException
    {
        for (PlogLCRTag tag : LCR.rawTags)
        {
            switch (tag.id)
            {
                case PlogLCRTag.TAG_FEATURES_1 :
                    int features = tag.valueInt();
                    this.plogDictCacheEnabled = (features
                            & PlogLCRTag.TAG_FEATURES_1_CACHE_DICT) > 0;
                    break;
                default :
                    /*
                     * no other feature flags known in this version; and ignore
                     * any non-flag tags
                     */
            }
        }
    }

    /**
     * Actually read one LCR from the plog, reading the correct number of bytes,
     * changing little endian file into big endian Java
     * 
     * @throws IOException
     */
    private PlogLCRTag readRawLCRTag() throws IOException
    {
        PlogLCRTag tag = new PlogLCRTag();
        tag.length = readSwappedInteger(plogStream);
        tag.id = readSwappedInteger(plogStream);
        tag.rawData = new int[tag.length - 2];
        for (int i = 0; i < tag.length - 2; i++)
            tag.rawData[i] = readSwappedInteger(plogStream);
        return tag;
    }

    /**
     * Set the last event processed and parse to determine internal position.
     * 
     * @param restartEventId last processed event id
     * @throws IOException
     */
    public void setLastEventId(String restartEventId) throws ReplicatorException
    {
        logger.info("Assigning the last processed event ID for restart: "
                + restartEventId);
        lastProcessedEventId = restartEventId;
        setInternalLastEventId(restartEventId);
    }

    /**
     * Parse lastEventId handed to us by PlogExtractor init, break it into
     * components and set our internal state.
     * 
     * @param restartEventId last processed event id
     * @throws IOException
     */
    private int setInternalLastEventId(String restartEventId)
            throws ReplicatorException
    {
        int firstSequence = 0;
        PlogEventId plogEventId = new PlogEventId(restartEventId);
        restartEventCommitSCN = plogEventId.getCommitSCN();
        restartEventXID = plogEventId.getXid();
        restartEventChangeSeq = plogEventId.getChangeSeq();
        restartEventStartSCN = plogEventId.getStartSCN();
        restartEventStartPlog = plogEventId.getStartPlog();
        firstSequence = restartEventStartPlog;

        return firstSequence;
    }

    /**
     * Get last obsolete plog, so we can signal mine. This accounts for the
     * number of retained plogs.
     */
    private int getLastObsoletePlog() throws ReplicatorException
    {
        // This should call the PluginContext to get the last committed process
        // id.
        if (lastProcessedEventId == null)
        {
            return 0;
        }
        else
        {
            PlogEventId plogEventId = new PlogEventId(lastProcessedEventId);
            int obsSequence = plogEventId.getStartPlog();
            if (obsSequence == Integer.MAX_VALUE)
            {
                // Can happen if we are still seeking a specific SCN position.
                return 0;
            }
            else
            {
                // This is a normal plog number. Subtract the retained plogs so
                // we don't delete prematurely and return.
                return Math.max(obsSequence - retainedPlogs, 0);
            }
        }
    }

    /**
     * Main loop of plog reader
     * 
     * @param context Tungsten plugin context
     * @param queue queue we use to return DBMS events
     * @param vmrrMgr Coordinator object for vmrr process
     * @param cache Vector cache to hold pending transactions
     */
    public PlogReaderThread(PluginContext context,
            BlockingQueue<DBMSEvent> queue, RedoReaderManager vmrrMgr,
            RawByteCache cache)
    {
        logger.info("PlogReaderThread: " + plogDirectory + "/"
                + sleepSizeInMilliseconds + "/" + transactionFragSize);

        this.context = context;
        this.queue = queue;
        this.vmrrMgr = vmrrMgr;
        this.tungstenSchema = context.getReplicatorSchemaName().toLowerCase();
        this.cache = cache;
    }

    public int getRetainedPlogs()
    {
        return retainedPlogs;
    }

    /** Sets the number of plogs to retain. */
    public void setRetainedPlogs(int retainedPlogs)
    {
        this.retainedPlogs = retainedPlogs;
    }

    public String getPlogDirectory()
    {
        return plogDirectory;
    }

    /** Sets the directory name where plog files are located. */
    public void setPlogDirectory(String plogDirectory)
    {
        this.plogDirectory = plogDirectory;
    }

    public int getSleepSizeInMilliseconds()
    {
        return sleepSizeInMilliseconds;
    }

    /** Sets the number of milliseconds to sleep when waiting for more data. */
    public void setSleepSizeInMilliseconds(int sleepSizeInMilliseconds)
    {
        this.sleepSizeInMilliseconds = sleepSizeInMilliseconds;
    }

    public int getHealthCheckInterval()
    {
        return healthCheckInterval;
    }

    /**
     * Sets the number of sleep cycles to wait before doing a replicator health
     * check.
     */
    public void setHealthCheckInterval(int healthCheckInterval)
    {
        this.healthCheckInterval = healthCheckInterval;
    }

    public int getTransactionFragSize()
    {
        return transactionFragSize;
    }

    /** Sets the number of LCRs per transaction fragment. */
    public void setTransactionFragSize(int transactionFragSize)
    {
        this.transactionFragSize = transactionFragSize;
    }

    /** Sets the number of LCRs to buffer per transaction. */
    public int getLcrBufferLimit()
    {
        return lcrBufferLimit;
    }

    public void setLcrBufferLimit(int lcrBufferLimit)
    {
        this.lcrBufferLimit = lcrBufferLimit;
    }

    /** Returns the last processed event ID. */
    public String getLastProcessedEventId()
    {
        return lastProcessedEventId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        try
        {
            // Run the task loop.
            runTask();
        }
        catch (InterruptedException e)
        {
            if (cancelled)
                logger.info("Oracle reader thread exiting normally");
            else
                logger.warn(
                        "Oracle reader thread was interrupted without cancellation request; exiting");
        }
        catch (ReplicatorException e)
        {
            logger.error("Oracle reader thread failed: " + e.getMessage());
            try
            {
                context.getEventDispatcher()
                        .put(new ErrorNotification(e.getMessage(), e));
            }
            catch (InterruptedException e1)
            {
                // If this happens it's because we should be quitting anyway.
                logger.warn(
                        "Oracle reader thread cancelled while signalling error");
            }
        }
        catch (Throwable e)
        {
            logger.error("Oracle reader thread failed: " + e.getMessage(), e);
            try
            {
                context.getEventDispatcher().put(new ErrorNotification(
                        "Oracle reader thread failed: " + e.getMessage(), e));
            }
            catch (InterruptedException e1)
            {
                // If this happens it's because we should be quitting anyway.
                logger.warn(
                        "Oracle reader thread cancelled while signalling error");
            }
        }
        finally
        {
            // Close current plog file, if any, to prevent file descriptor
            // leaks.
            closeFile();
        }
    }

    /**
     * Thread main loop. Open the plogs, read them, parse the changes, on commit
     * send the changes through queue to PlogExtractor
     */
    private void runTask() throws InterruptedException, IOException,
            SerialException, ReplicatorException, SQLException
    {
        // TODO Review
        // Skip events is used to tell whether we should skip events when this starts up.
    	// By default, this is off except if lastEvent was set.
    	boolean skipEvents = false;
    	
        // Ensure the replicator is registered with the redo reader.
        vmrrMgr.registerExtractor();

        int firstSequence = Integer.MAX_VALUE;
        // LinkedList<PLogLCR> listOfLCR = new LinkedList<PlogLCR>();
        ArrayList<PlogTransaction> tranAtSameSCN = new ArrayList<PlogTransaction>();
        long SCNAtSameSCN = 0;

        // See if we can find a restart position.
        if(lastEvent != null)
        {
            // TODO Review
            // last event is set which means we are auto-repositionning from
            // this last received event. We are going to skip events until we
            // located this last one into the plogs
            firstSequence = Integer.MAX_VALUE;
            skipEvents = true;
        }
        else if (lastProcessedEventId != null)
        {
            firstSequence = setInternalLastEventId(lastProcessedEventId);
            if (logger.isDebugEnabled())
            {
                logger.debug(String.format(
                        "Starting from an existing event ID: lastProcessedEventId=%s firstSequence=%d",
                        lastProcessedEventId, firstSequence));
            }
        }
        else
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(
                        "Restart position not set, searching for oldest available plog");
            }
        }

        // If we don't have a plog ID we need to find one.
        // TODO Review
        // This looks like a bug in current code : 
        // retries should be initialized outside of the loop 
        int retries = 0;
        while (firstSequence == Integer.MAX_VALUE)
        {
            if (retries == 0)
            {
                // Just print this once.
                logger.info("Seeking oldest plog file to start extraction");
            }

            // Find oldest plog; check for interrupt on thread
            // thereafter.
            firstSequence = findOldestPlogSequence(plogDirectory);
            if (Thread.currentThread().isInterrupted())
            {
                throw new InterruptedException();
            }

            if (firstSequence < Integer.MAX_VALUE)
            {
                logger.info("Oldest plog found on disk: " + firstSequence);
                break;
            }
            // Bide a wee. If interrupted we just throw the exception
            // and exit routine.
            retries++;
            Thread.sleep(sleepSizeInMilliseconds);

            checkRedoReaderState(retries);
        }

        // Get the starting plog file.
        plogFilename = findMostRecentPlogFile(plogDirectory, firstSequence);

        // This will be overwritten by parsing the header - that is, if it's
        // already in the file.
        plogId = firstSequence;

        while (!cancelled)
        {
            // Report the last plog so that the MINE can clean up.
            int lastObsolePlog = getLastObsoletePlog();
            if (lastReportedObsolePlogSeq != lastObsolePlog
                    && lastObsolePlog > 0)
            {
                vmrrMgr.reportLastObsoletePlog(lastObsolePlog);
                lastReportedObsolePlogSeq = lastObsolePlog;
            }
            openFile(plogFilename);
            readHeader();
            int retryCount = 0;

            while (!cancelled)
            {
                PlogLCR r1 = null;
                while (r1 == null)
                {
                    try
                    {
                        r1 = readRawLCR();

                        if (skipEvents)
                        {
                            // TODO Review
                            // We are skipping events. Check when we have DML
                            // into TREP_COMMIT_SEQNO table whether it is the
                            // expected searched event id.
                            if (tungstenSchema.equalsIgnoreCase(r1.tableOwner)
                                    && SqlCommitSeqno.TABLE_NAME
                                            .equalsIgnoreCase(r1.tableName)
                                    && (r1.type == PlogLCR.ETYPE_LCR_DATA)
                                    && (r1.subtype != PlogLCR.ESTYPE_LCR_DDL))
                            {
                                // Found a candidate.. Parse it and check
                                // whether it is the expected one
                                DBMSData dbmsData = PlogTransaction
                                        .convertLCRtoDBMSDataDML(r1);
                                if (dbmsData instanceof RowChangeData)
                                {
                                    RowChangeData data = (RowChangeData) dbmsData;
                                    ArrayList<ColumnSpec> specs = data
                                            .getRowChanges().get(0)
                                            .getColumnSpec();
                                    ColumnSpec spec;
                                    int i;
                                    for (i = 0; i < specs.size(); i++)
                                    {
                                        spec = specs.get(i);
                                        if (spec.getName()
                                                .equalsIgnoreCase("EVENTID"))
                                        {
                                            break;
                                        }
                                    }

                                    if (i < specs.size())
                                    {
                                        ColumnVal columnVal = data
                                                .getRowChanges().get(0)
                                                .getColumnValues().get(0)
                                                .get(i);
                                        if (columnVal
                                                .getValue() instanceof String)
                                        {
                                            String value = (String) columnVal
                                                    .getValue();
                                            if (value.equals(
                                                    lastEvent.getEventId()))
                                            {
                                                logger.info("Searching for "
                                                        + lastEvent.getEventId()
                                                        + " - Found : "
                                                        + r1);
                                                r1 = null;
                                                skipEvents = false;
                                            }
                                            else
                                            {
                                                // TODO Review
                                            	// Check whether we can compare event id.
                                                // If we did not find the event
                                                // id and we are already too
                                                // far, we should probably fail
                                                // here!
                                                // For now, just consider that
                                                // if we did not find, we are
                                                // not yet at the correct point.
                                                r1 = null;
                                                continue;
                                            }                                            
                                        }
                                        else
                                        {
                                        	// TODO Review
                                        	// CONT-1560
                                            // If columnVal.getValue() was not a
                                            // string (null for example), this
                                            // was creating a transaction that
                                            // would remain opened forever
                                            r1 = null;
                                            continue;                                            
                                        }
                                    }
                                    else
                                    {
                                        r1 = null;
                                        continue;                                            
                                    }                                        
                                }
                            }
                            else if (r1.type == PlogLCR.ETYPE_CONTROL)
                            {
                                break;
                            }
                            else
                            {
                                //logger.warn("Found type " + r1.type + " subtype " + r1.subtype + " - skipping.");
                                r1 = null;
                                continue;
                            }
                        }
                        else
                        {
                            // Test this update to see if it is a change to the
                            // Tungsten TREP_COMMIT_SEQNO table, which should
                            // not be replicated.
                            if (tungstenSchema != null)
                            {
                                if (tungstenSchema
                                        .equalsIgnoreCase(r1.tableOwner)
                                        && "trep_commit_seqno"
                                                .equalsIgnoreCase(r1.tableName))
                                {
                                    if (logger.isDebugEnabled())
                                    {
                                        logger.debug(
                                                "Ignoring update to trep_commit_seqno: scn="
                                                        + r1.LCRid);
                                    }
                                    r1 = null;
                                    continue;
                                }
                            }
                        }
                    }
                    catch (java.io.EOFException e)
                    {
                        // wait for more data; also check if newer file is there
                        if (currentlyInIFILE)
                        {
                            // we process only complete IFILEs, so this
                            // should not ever happen.
                            throw new FileNotFoundException(
                                    currentIncludePlog.plogFilename
                                            + " ended prematurely, file is corrupted.");
                        }

                        // Check for a newer plog file every 5 retries.
                        if ((retryCount % 5) == 0)
                        {
                            String latestPlogFilename = findMostRecentPlogFile(
                                    plogDirectory, plogId);
                            if (!plogFilename.equals(latestPlogFilename))
                            {
                                // newer file available.
                                retryCount = 0;
                                closeFile();

                                // throw away all LCRs from open transactions.
                                throwAwayAllLCRsInPlog(plogId);
                                setInternalLastEventId(lastProcessedEventId);
                                plogFilename = latestPlogFilename;
                                openFile(plogFilename);
                                readHeader();
                            }
                        }


                        checkRedoReaderState(retryCount);

                        // Update retries and sleep.
                        retryCount++;
                        Thread.sleep(sleepSizeInMilliseconds);
                        if (logger.isDebugEnabled())
                            logger.debug("Wait for more data in plog.");
                    }
                }
                // TODO Review
                // Set retry count to 0 to avoid misleading messages
                retryCount = 0;
                
                // This is header information for the plog. We just read it to
                // find out which features are enabled.
                if (r1.type == PlogLCR.ETYPE_CONTROL
                        && r1.subtype == PlogLCR.ESTYPE_HEADER)
                {
                    // plog header, read features etc.
                    readControlHeader(r1);
                    logger.info(
                            "Plog dict cache: " + this.plogDictCacheEnabled);
                }

                // This is the footer of the plog. Time to rotate over to the
                // next file.
                if (r1.type == PlogLCR.ETYPE_CONTROL
                        && r1.subtype == PlogLCR.ESTYPE_FOOTER
                        && !currentlyInIFILE)
                {
                    // plog footer
                    break;
                }

                if (r1.type == PlogLCR.ETYPE_LCR_PLOG)
                {
                    // IFiles are referenced plogs that contain extracted data
                    // used to provision full tables.
                    //
                    // Ifiles are not currently supported in the extractor, so
                    // we just wait for the end and return to the parent plog.
                    if (r1.subtype == PlogLCR.ESTYPE_LCR_PLOG_IFILE)
                    {
                        // This is the header information, which we parse.
                        currentIncludePlog = r1.parseIncludePlogLCR(this);
                    }
                    else if (r1.subtype == PlogLCR.ESTYPE_LCR_PLOG_IFILE_STATS)
                    {
                        // At this point we have the full file stats.
                        plogStreamPaused = plogStream;
                        openFile(currentIncludePlog.plogFilename);
                        readHeader();
                        currentlyInIFILE = true;
                    }
                }
                if (r1.type == PlogLCR.ETYPE_CONTROL
                        && r1.subtype == PlogLCR.ESTYPE_FOOTER
                        && currentlyInIFILE)
                {
                    // We have finished the IFILE. Close and return to the
                    // parent plog.
                    closeFile();
                    currentlyInIFILE = false;
                    plogStream = plogStreamPaused;
                }

                // We have finished plog housekeeping and are now processing
                // real transactions.

                if (r1.SCN > SCNAtSameSCN && !tranAtSameSCN.isEmpty())
                {
                    // The current SCN has moved past the commit SCN of the
                    // pending batch of transactions. We must release the queued
                    // transactions now.

                    // Sort transactions by the XID. This ensures a
                    // deterministic ordering in case the replicator crashes or
                    // stops before pending transactions reach the THL. This
                    // also ensures that the ordering is deterministic even if
                    // the mine process restarts or RAC is involved.
                    Collections.sort(tranAtSameSCN);

                    // Find the oldest open plog. This will be used to set the
                    // plog position for restart.
                    long oldestPlogId = minimalPlogIdInOpenTransactions();

                    // Process DDL transactions first. This takes care of
                    // the case where a DDL operation like CREATE TABLE xxx
                    // FROM SELECT also generates DML. The split ensures we get
                    // the DDL first.
                    for (PlogTransaction t : tranAtSameSCN)
                    {
                        if (!t.transactionIsDML)
                        {
                            lastProcessedEventId = t.pushContentsToQueue(queue,
                                    minimalSCNInOpenTransactions(),
                                    transactionFragSize, oldestPlogId - 1);
                            openTransactions.remove(t.XID);
                            t.release();
                        }
                    }

                    // Process DML transactions now that DDL has safely reached
                    // the log.
                    for (PlogTransaction t : tranAtSameSCN)
                    {
                        if (t.transactionIsDML)
                        {
                            lastProcessedEventId = t.pushContentsToQueue(queue,
                                    minimalSCNInOpenTransactions(),
                                    transactionFragSize, oldestPlogId - 1);
                            openTransactions.remove(t.XID);
                            t.release();
                        }
                    }
                    tranAtSameSCN.clear();
                }

                // Transactional changes always have an XID but some LCRs may
                // not because they are housekeeping operations. So if we have
                // an XID that means we need to do something.
                if (r1.XID != null)
                {
                    PlogTransaction t = openTransactions.get(r1.XID);
                    if (t == null)
                    {
                        // TODO: two transactions can have same XID due to
                        // splitting of redo reader splitting of DML and DDL.
                        // This needs to be fixed in the redo reader.
                        t = new PlogTransaction(cache, r1.XID, lcrBufferLimit);
                        openTransactions.put(r1.XID, t);
                    }
                    t.putLCR(r1);
                    if (t.isCommitted())
                    {
                        if (t.isEmpty())
                        {
                            // This either a housekeeping transaction or a
                            // transaction on a table we don't replicate.
                            // Throw it away.
                            openTransactions.remove(r1.XID);
                            t.release();
                        }
                        else if (t.commitSCN < restartEventCommitSCN)
                        {
                            // This is a transaction that is before our restart
                            // point. We can safely throw it away.
                            if (logger.isDebugEnabled())
                            {
                                logger.info(r1.XID + " committed at "
                                        + t.commitSCN + ", before restart SCN "
                                        + restartEventCommitSCN);
                            }
                            openTransactions.remove(r1.XID);
                            t.release();
                        }
                        else if (t.commitSCN == restartEventCommitSCN
                                && t.XID.compareTo(restartEventXID) < 0)
                        {
                            // We have a transaction at the same SCN *but* the
                            // XID is lower than the last XID that made it into
                            // the log, because we sort by XID before posting
                            // transactions at the same commitSCN. We throw
                            // it away.
                            //
                            // TODO: This exposes a bug. Because we split DDL
                            // and DML that means there are two XID orderings in
                            // the transaction.
                            logger.info(r1.XID + " committed at " + t.commitSCN
                                    + "= restart SCN " + restartEventCommitSCN
                                    + ", but it's before restart XID "
                                    + restartEventXID);
                            openTransactions.remove(r1.XID);
                            t.release();
                        }
                        else if (t.commitSCN == restartEventCommitSCN
                                && t.XID.equals(restartEventXID))
                        {
                            // We are looking at the last transaction that was
                            // committed upstream to the log.
                            logger.info(">>>" + r1.XID + " committed at "
                                    + t.commitSCN + "= restart SCN "
                                    + restartEventCommitSCN
                                    + ", exactly restart XID "
                                    + restartEventXID);
                            if (restartEventChangeSeq.equals("LAST"))
                            {
                                // We are on the last fragment of the
                                // transaction that was at the restart and is
                                // the last event applied. This transaction must
                                // now be discarded.
                                logger.info(">>> was complete last time");
                                openTransactions.remove(r1.XID);
                                t.release();
                            }
                            else
                            {
                                // This is a fragment that was already applied,
                                // so skip this fragment. We do not need to
                                // apply it because it should already have
                                // been applied.
                                t.setSkipSeq(Integer
                                        .parseInt(restartEventChangeSeq));
                                tranAtSameSCN.add(t);
                                SCNAtSameSCN = t.commitSCN;
                            }
                        }
                        else
                        {
                            // This is a normal transaction past the restart
                            // point that should be committed. Add it to the
                            // transactions to commit at this SCN.
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("XID: " + t.XID);
                            }
                            t.setSkipSeq(0);
                            tranAtSameSCN.add(t);
                            SCNAtSameSCN = t.commitSCN;
                        }
                    }
                }
            }
            closeFile();

            plogFilename = null;
            while (plogFilename == null && !cancelled)
            {
                try
                {
                    plogFilename = findMostRecentPlogFile(plogDirectory,
                            plogId + 1);
                }
                catch (FileNotFoundException e)
                {
                    logger.info("Waiting for next file to appear: "
                            + e.getMessage());
                }

                // Bide a wee.
                Thread.sleep(sleepSizeInMilliseconds);
            }

            // this will be overwritten by parsing the header -
            // that is, if it's already in the file
            plogId++;
        }
    }

    private void checkRedoReaderState(int retries) throws ReplicatorException
    {
        // Check for a dead redo reader at each health check interval.
        if (retries > 0 && (retries % healthCheckInterval) == 0)
        {
            logger.info(
                    "No plog found, checking redo reader status: retries="
                            + retries);
            if (vmrrMgr.isRunning())
            {
                // If it is running, do a health check.
                vmrrMgr.healthCheck();
            }
            else
            {
                // If it is not running, try to restart.
                logger.warn(
                        "Redo reader process appears to have failed; attempting restart");
                vmrrMgr.start();
            }
        }
    }

    /**
     * Get smallest SCN among all open transactions
     * 
     * @return minimal SCN
     */
    private long minimalSCNInOpenTransactions()
    {
        Collection<PlogTransaction> allTrans = openTransactions.values();
        long minSCN = Long.MAX_VALUE;
        for (PlogTransaction tran : allTrans)
        {
            if (tran.startSCN < minSCN)
            {
                minSCN = tran.startSCN;
            }
        }
        return minSCN;
    }

    /**
     * Get smallest start plog id among all open transactions
     * 
     * @return minimal plog if
     */
    private long minimalPlogIdInOpenTransactions()
    {
        Collection<PlogTransaction> allTrans = openTransactions.values();
        long minPlogId = Long.MAX_VALUE;
        for (PlogTransaction tran : allTrans)
        {
            if (tran.startPlogId > 0 && tran.startPlogId < minPlogId)
            {
                minPlogId = tran.startPlogId;
            }
        }
        return minPlogId;
    }

    /**
     * Throw away all LCRs in open transactions from this plog id (plog id is
     * supposed to be current, but any will actually work) Principle of
     * operation: simulate a rollback to savepoint to start of that plog.
     * 
     * @param plogId seq# of plog
     */
    private void throwAwayAllLCRsInPlog(int plogId) throws ReplicatorException
    {
        Collection<PlogTransaction> allTrans = openTransactions.values();

        PlogLCR rollLCR = new PlogLCR();
        rollLCR.type = PlogLCR.ETYPE_TRANSACTIONS;
        rollLCR.subtype = PlogLCR.ESTYPE_TRAN_ROLLBACK_TO_SAVEPOINT;
        rollLCR.LCRSavepointId = plogId * 1000000000L;

        for (PlogTransaction tran : allTrans)
        {
            if (!tran.isEmpty())
            {
                tran.putLCR(rollLCR);
            }
        }
    }

    /**
     * Request thread to stop. This sets the cancelled flag and posts an
     * interrupt.
     * 
     * @throws InterruptedException
     */
    public synchronized void cancel() throws InterruptedException
    {
        logger.info("Cancelling redo reader thread...");
        this.cancelled = true;
        this.interrupt();
        this.join(30000);
        if (this.isAlive())
        {
            logger.warn(
                    "Redo reader thread did not respond to cancellation...");
        }
    }

    /**
     * Prepare the processing. Does nothing as of now.
     */
    public void prepare()
    {
    }

    public void setLastEvent(ReplDBMSHeader lastEvent)
    {
        this.lastEvent = lastEvent;
    }
}
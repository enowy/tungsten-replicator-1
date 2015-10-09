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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.cache.RawByteCache;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
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

    private String       plogDirectory;
    private int          sleepSizeInMilliseconds;
    private int          transactionFragSize = 0;
    private String       replicateConsoleScript;
    private String       replicateApplyName;
    private String       tungstenSchema;
    private RawByteCache cache;

    /*
     * dict cache enabled / disabled for current plog
     */
    private boolean             plogDictCacheEnabled;
    HashMap<Integer, String>    plogDictCacheObjectOwner = new HashMap<Integer, String>();
    HashMap<Integer, String>    plogDictCacheObjectName  = new HashMap<Integer, String>();
    HashMap<String, PlogLCRTag> plogDictCacheColumnName  = new HashMap<String, PlogLCRTag>();
    HashMap<String, PlogLCRTag> plogDictCacheColumnType  = new HashMap<String, PlogLCRTag>();

    private String lastProcessedEventId = null;

    private boolean cancelled;

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
     * Reads a unsigned integer (32-bit) from an InputStream. The value is
     * converted to the opposed endian system while reading.
     * 
     * @param input source InputStream
     * @return the value just read
     * @throws IOException in case of an I/O problem
     */
    private static long readSwappedUnsignedInteger(final DataInputStream input)
            throws IOException
    {
        final int value1 = input.readByte();
        final int value2 = input.readByte();
        final int value3 = input.readByte();
        final int value4 = input.readByte();

        final long low = (((value1 & 0xff) << 0) + ((value2 & 0xff) << 8)
                + ((value3 & 0xff) << 16));

        final long high = value4 & 0xff;

        return (high << 24) + (0xffffffffL & low);
    }

    /**
     * Converts a "int" value between endian systems.
     * 
     * @param value value to convert
     * @return the converted value
     */
    private static int swapInteger(final int value)
    {
        return (((value >> 0) & 0xff) << 24) + (((value >> 8) & 0xff) << 16)
                + (((value >> 16) & 0xff) << 8) + (((value >> 24) & 0xff) << 0);
    }

    /**
     * For start with no eventId, search a directory and find oldest (=lowest
     * sequence) plog
     * 
     * @param path Directory to search
     * @return first sequence found. Integer.MAX_VALUE if no file present at
     *         all.
     */
    private static int findOldestPlogSequence(String path)
            throws FileNotFoundException
    {
        if (path == null)
        {
            throw new NullPointerException(
                    "plogDirectory was not set in properties file");
        }

        if (logger.isDebugEnabled())
            logger.debug("findOldestPlogSequence: " + path);

        int rtval = Integer.MAX_VALUE;
        File dir = new File(path);
        if (!dir.canRead())
        {
            throw new FileNotFoundException(path + " is not accessible");
        }
        File[] matchingFiles = dir.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.toLowerCase().contains(".plog.");
            }
        });
        for (File mf : matchingFiles)
        {
            String p = null;
            try
            {
                p = mf.getName();
                p = p.substring(0, p.indexOf(".plog."));
                int s = Integer.parseInt(p);
                if (s < rtval)
                {
                    rtval = s;
                }
            }
            catch (NumberFormatException e)
            {
                logger.warn(p + " is not really a time number");
            }
            catch (IndexOutOfBoundsException e)
            {
                logger.warn(p + " malformed, did not find .plog extension.");
            }
        }
        return rtval;
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
                return name.toLowerCase().startsWith(prefix);
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
    }

    /**
     * Close plog file
     * 
     * @throws IOException
     */
    private void closeFile() throws IOException
    {
        plogStream.close();
        logger.info("File " + plogFilename + " closed.");
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
                    rawLCR.LCRid = tag.valueLong()
                            + 1000000000L/* 1e9 */ * plogId;
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
                else /* will be processed later */
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

        if (restartEventId != null)
        {
            try
            {
                String[] eventItems = restartEventId.split("#");
                restartEventCommitSCN = Long.parseLong(eventItems[0]);
                restartEventXID = eventItems[1];
                restartEventChangeSeq = eventItems[2];
                restartEventStartSCN = Long.parseLong(eventItems[3]);
                restartEventStartPlog = Integer.parseInt(eventItems[4]);
                firstSequence = restartEventStartPlog;
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                throw new ReplicatorException(
                        "Malformed eventId " + restartEventId
                                + " - must be 5 values delimited by # signs.");
            }
            catch (NumberFormatException e)
            {
                throw new ReplicatorException(
                        "Malformed eventId " + restartEventId
                                + " - must be 5 values delimited by # signs.");
            }
        }

        return firstSequence;
    }

    /**
     * Get last obsolete plog, so we can signal mine it FIXME It should do it
     * from last applied, not last processed event id. We are waiting to get API
     * for that.
     * 
     * @throws IOException
     */
    private int getLastObsoletePlog() throws ReplicatorException
    {
        /*
         * FIXME this is supposed to call Tungsten internal API to get last
         * eventId actually applied
         */
        String lastAppliedEventId = this.lastProcessedEventId;
        int obsSequence = 0;

        if (lastAppliedEventId != null)
        {
            try
            {
                String[] eventItems = lastAppliedEventId.split("#");
                obsSequence = Integer.parseInt(eventItems[4]);
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                throw new ReplicatorException(
                        "Malformed eventId " + lastAppliedEventId
                                + " - must be 5 values delimited by # signs.");
            }
            catch (NumberFormatException e)
            {
                throw new ReplicatorException(
                        "Malformed eventId " + lastAppliedEventId
                                + " - must be 5 values delimited by # signs.");
            }
        }
        return obsSequence;
    }

    /**
     * Ask mine to register our extractor, so it can then set last obsolete. We
     * call this on every start of extractor, as it is harmless to do it
     * repeatedly.
     * 
     * @throws IOException
     */
    private boolean registerThisExtractorAtMine()
    {
        try
        {
            Process process = Runtime.getRuntime()
                    .exec(new String[]{replicateConsoleScript,
                            "PROCESS DISCONNECTED_APPLY REGISTER "
                                    + this.replicateApplyName});
            process.waitFor();
            return true;
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * Signal mine our last obsolete plog.
     * 
     * @param newObsolete last obsolete plog sequence to set
     * @throws IOException
     */
    private boolean reportLastObsoletePlog(int newObsolete)
    {
        try
        {
            Process process = Runtime.getRuntime()
                    .exec(new String[]{replicateConsoleScript,
                            "PROCESS DISCONNECTED_APPLY SET_OBSOLETE "
                                    + this.replicateApplyName + " "
                                    + newObsolete});
            return true;
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * Main loop of plog reader
     * 
     * @param context Tungsten pluging context
     * @param queue queue we use to return DBMS Events
     * @param plogDirectory Location of plog files (directory)
     * @param sleepSizeInMilliseconds property set by user (how much to sleep
     *            waiting for data)
     * @param transactionFragSize property set by user (how to fragment large
     *            transactions)
     * @param replicateConsoleScript property set by user (start-console.sh)
     * @param replicateApplyName property set by user (name of apply)
     * @param cache Vector cache to hold pending transactions
     * @throws IOException We can fail on disk open or charset conversion
     */
    public PlogReaderThread(PluginContext context,
            BlockingQueue<DBMSEvent> queue, String plogDirectory,
            int sleepSizeInMilliseconds, int transactionFragSize,
            String replicateConsoleScript, String replicateApplyName,
            RawByteCache cache)
    {

        logger.info("PlogReaderThread: " + plogDirectory + "/"
                + sleepSizeInMilliseconds + "/" + transactionFragSize);

        this.plogDirectory = plogDirectory;
        this.sleepSizeInMilliseconds = sleepSizeInMilliseconds;
        this.queue = queue;
        this.transactionFragSize = transactionFragSize;
        this.replicateConsoleScript = replicateConsoleScript;
        this.replicateApplyName = replicateApplyName;
        this.tungstenSchema = context.getReplicatorSchemaName().toLowerCase();
        this.cache = cache;

        this.registerThisExtractorAtMine();
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
        runTask();
    }

    /**
     * Thread main loop. Open the plogs, read them, parse the changes, on commit
     * send the changes through queue to PlogExtractor
     */
    private void runTask()
    {
        try
        {
            int firstSequence = Integer.MAX_VALUE;
            // LinkedList<PLogLCR> listOfLCR = new LinkedList<PlogLCR>();
            ArrayList<PlogTransaction> tranAtSameSCN = new ArrayList<PlogTransaction>();
            long SCNAtSameSCN = 0;

            if (lastProcessedEventId != null)
            {
                firstSequence = setInternalLastEventId(lastProcessedEventId);
            }
            else
            {
                while (firstSequence == Integer.MAX_VALUE)
                {
                    firstSequence = findOldestPlogSequence(plogDirectory);
                    if (firstSequence < Integer.MAX_VALUE)
                    {
                        logger.info(
                                "Oldest plog found on disk: " + firstSequence);
                        break;
                    }
                    logger.info("No plog found on disk, sleep and retry.");
                    try
                    {
                        Thread.sleep(sleepSizeInMilliseconds);
                    }
                    catch (InterruptedException ie)
                    {
                        // does not matter if sleep is interrupted
                    }
                }
            }

            plogFilename = findMostRecentPlogFile(plogDirectory, firstSequence);
            plogId = firstSequence; /*
                                     * this will be overwritten by parsing the
                                     * header - that is, if it's already in the
                                     * file
                                     */

            while (!cancelled)
            {
                int lastObsolePlog = getLastObsoletePlog();
                if (lastReportedObsolePlogSeq != lastObsolePlog
                        && lastObsolePlog > 0)
                {
                    if (reportLastObsoletePlog(lastObsolePlog))
                    {
                        lastReportedObsolePlogSeq = lastObsolePlog;
                    }
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
                        catch (java.io.EOFException e)
                        { /*
                           * wait for more data; also check if newer file is
                           * there
                           */
                            if (currentlyInIFILE)
                            { /*
                               * we process only complete IFILEs, so this should
                               * not ever happen
                               */
                                throw new FileNotFoundException(
                                        currentIncludePlog.plogFilename
                                                + " ended prematurely, file is corrupted.");
                            }
                            if ((retryCount % 5) == 0)
                            {
                                String latestPlogFilename = findMostRecentPlogFile(
                                        plogDirectory, plogId);
                                if (!plogFilename.equals(latestPlogFilename))
                                { /* newer file available */
                                    retryCount = 0;
                                    closeFile();
                                    throwAwayAllLCRsInPlog(
                                            plogId); /*
                                                      * throw away all LCRs from
                                                      * open transactions
                                                      */
                                    setInternalLastEventId(
                                            lastProcessedEventId);
                                    plogFilename = latestPlogFilename;
                                    openFile(plogFilename);
                                    readHeader();
                                }
                            }
                            retryCount++;
                            try
                            {
                                Thread.sleep(sleepSizeInMilliseconds);
                            }
                            catch (InterruptedException ie)
                            {
                                // does not matter if sleep is interrupted
                            }
                            logger.debug("Wait for more data in plog.");
                        }
                    }

                    if (r1.type == PlogLCR.ETYPE_CONTROL
                            && r1.subtype == PlogLCR.ESTYPE_HEADER)
                    { // plog header, read features etc.
                        readControlHeader(r1);
                        logger.info("Plog dict cache: "
                                + this.plogDictCacheEnabled);
                    }

                    if (r1.type == PlogLCR.ETYPE_CONTROL
                            && r1.subtype == PlogLCR.ESTYPE_FOOTER
                            && !currentlyInIFILE)
                    {
                        break;
                    } // plog footer

                    if (r1.type == PlogLCR.ETYPE_LCR_PLOG)
                    { /* Include PLOG (LOAD) */
                        /*
                         * Note that we wait for complete file. We do it by
                         * waiting for end of ifile in the parent plog
                         */
                        if (r1.subtype == PlogLCR.ESTYPE_LCR_PLOG_IFILE)
                        {
                            currentIncludePlog = r1.parseIncludePlogLCR(this); // just
                                                                               // parse
                                                                               // the
                                                                               // info
                        }
                        else
                            if (r1.subtype == PlogLCR.ESTYPE_LCR_PLOG_IFILE_STATS)
                        {
                            plogStreamPaused = plogStream;
                            openFile(currentIncludePlog.plogFilename);
                            readHeader();
                            currentlyInIFILE = true;
                        }
                    }
                    if (r1.type == PlogLCR.ETYPE_CONTROL
                            && r1.subtype == PlogLCR.ESTYPE_FOOTER
                            && currentlyInIFILE)
                    { // close IFILE, back to parent
                        closeFile();
                        currentlyInIFILE = false;
                        plogStream = plogStreamPaused;
                    }

                    if (r1.SCN > SCNAtSameSCN && !tranAtSameSCN.isEmpty())
                    {
                        /*
                         * we are at next SCN past last Commit, release the
                         * queued transactions. And order them by XID - that's
                         * the only reason why we don't output them right away.
                         * Just to make sure the ordering will never change,
                         * even if mine is restarted, RAC is involved and
                         * whatever. To sort DDL transactions *before* any DML
                         * (think of Create Table as select), process them
                         * first.
                         */
                        Collections.sort(tranAtSameSCN);
                        long oldestPlogId = minimalPlogIdInOpenTransactions();
                        for (PlogTransaction t : tranAtSameSCN)
                        {
                            if (!t.transactionIsDML)
                            {
                                lastProcessedEventId = t.pushContentsToQueue(
                                        queue, minimalSCNInOpenTransactions(),
                                        transactionFragSize, oldestPlogId - 1);
                                openTransactions.remove(t.XID);
                                t.release();
                            }
                        }
                        for (PlogTransaction t : tranAtSameSCN)
                        {
                            if (t.transactionIsDML)
                            {
                                lastProcessedEventId = t.pushContentsToQueue(
                                        queue, minimalSCNInOpenTransactions(),
                                        transactionFragSize, oldestPlogId - 1);
                                openTransactions.remove(t.XID);
                                t.release();
                            }
                        }
                        tranAtSameSCN.clear();
                    }

                    if (r1.XID != null)
                    {
                        PlogTransaction t = openTransactions.get(r1.XID);
                        if (t == null)
                        {
                            t = new PlogTransaction(cache, r1.XID);
                            openTransactions.put(r1.XID, t);
                        }
                        t.putLCR(r1);
                        if (t.isCommitted())
                        {
                            if (t.isEmpty())
                            {
                                // Empty transaction, throw way.
                                openTransactions.remove(r1.XID);
                                t.release();
                            }
                            else if (t.commitSCN < restartEventCommitSCN)
                            {
                                // Pre-restart transaction, throw away.
                                logger.info(r1.XID + " committed at "
                                        + t.commitSCN + ", before restart SCN "
                                        + restartEventCommitSCN);
                                openTransactions.remove(r1.XID);
                                t.release();
                            }
                            else
                                if (t.commitSCN == restartEventCommitSCN
                                        && t.XID.compareTo(restartEventXID) < 0)
                            {
                                // Pre-restart transaction, throw away.
                                logger.info(r1.XID + " committed at "
                                        + t.commitSCN + "= restart SCN "
                                        + restartEventCommitSCN
                                        + ", but it's before restart XID "
                                        + restartEventXID);
                                openTransactions.remove(r1.XID);
                                t.release();
                            }
                            else
                                    if (t.commitSCN == restartEventCommitSCN
                                            && t.XID.equals(restartEventXID))
                            {
                                logger.info(">>>" + r1.XID + " committed at "
                                        + t.commitSCN + "= restart SCN "
                                        + restartEventCommitSCN
                                        + ", exactly restart XID "
                                        + restartEventXID);
                                if (restartEventChangeSeq.equals("LAST"))
                                {
                                    logger.info(">>> was complete last time");
                                }
                                else
                                {
                                    t.setSkipSeq(Integer
                                            .parseInt(restartEventChangeSeq));
                                    tranAtSameSCN.add(t);
                                    SCNAtSameSCN = t.commitSCN;
                                }
                            }
                            else
                            {
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
                while (plogFilename == null)
                {
                    try
                    {
                        plogFilename = findMostRecentPlogFile(plogDirectory,
                                plogId + 1);
                    }
                    catch (FileNotFoundException e)
                    {
                        logger.error("Waiting for next file to appear: "
                                + e.getMessage());
                    }
                    try
                    {
                        Thread.sleep(sleepSizeInMilliseconds);
                    }
                    catch (InterruptedException ie)
                    {
                        // does not matter if sleep is interrupted
                    }
                }
                plogId++; /*
                           * this will be overwritten by parsing the header -
                           * that is, if it's already in the file
                           */
            }
        }
        catch (Throwable e)
        {
            try
            {
                queue.put(new PlogDBMSErrorMessage(
                        "Oracle Reader thread failed", e));

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String exceptionAsString = sw.toString();

                logger.error("Oracle Reader thread failed: " + e
                        + exceptionAsString);
                System.out.println(e);
            }
            catch (InterruptedException ignore)
            {
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
     * Request thread to stop.
     */
    public void cancel()
    {
        this.cancelled = true;
    }

    /**
     * Prepare the processing. Does nothing as of now.
     */
    public void prepare()
    {
    }

}
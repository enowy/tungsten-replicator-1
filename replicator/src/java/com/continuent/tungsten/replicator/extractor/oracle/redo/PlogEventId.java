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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Vit Spinka, Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Parses an event ID for Oracle and returns the components thereof. Oracle
 * event IDs consist of the following components.
 * <ul>
 * <li>commitScn is the commit SCN that defines serial ordering. More than one
 * Xact may have the same commit SCN.</li>
 * <li>XID - a unique transaction ID. XIDs have a hex-numeric format like
 * 001.002.033404. As explained below this may have a prefix in some cases.</li>
 * <li>changeSeq - the ID of successive LCRs in the same transaction. The last
 * LCR in sequence is labeled “LAST."</li>
 * <li>startScn - the starting SCN, assigned by Oracle when the transaction
 * starts.</li>
 * <li>startPlog - the oldest active plog at the time this transaction commits
 * and determines where we begin to scan on restart. This is set to
 * Integer.MAX_VALUE when processing a restart SCN value, which forces a search
 * of plog files for the earliest one</li>
 * </ul>
 * XIDs are specially formatted to deal with the case where a single
 * "transaction" within Oracle combines both schema (DDL) and data (DML)
 * changes. The XID strings therefore can have the following prefixes.
 * <ul>
 * <li>#DDL (pre) - DDL that should happen before DML</li>
 * <li>DDL (post) - DDL that should happen after DML</li>
 * </ul>
 * XIDs for ordinary DML operations have no prefix. The prefixes are formed in
 * such a way that XIDs for transactions sort into correct order for apply and
 * should be so ordered in the transaction history log.
 */
public class PlogEventId extends Thread
{
    private String eventId = null;

    private long   commitSCN = 0;
    private String xid       = "";
    private String changeSeq = "";
    private long   startSCN  = 0;
    private int    startPlog = 0;

    public PlogEventId(String eventId) throws ReplicatorException
    {
        this.eventId = eventId;

        // Parse and store event ID values based on the pattern.
        String[] eventItems = eventId.split("#");
        if (eventItems.length == 1)
        {
            // Restart SCN. Fill in dummy values for xid and later values. Plog
            // is set to max integer value which forces a search.
            String scn = eventItems[0];
            if ("NOW".compareToIgnoreCase(scn) == 0)
            {
                // This is the current SCN. Any value will do when reading.
                this.commitSCN = 0;
            }
            else
            {
                // This is a real SCN.
                this.commitSCN = parseLong(eventItems[0]);
            }

            this.startPlog = Integer.MAX_VALUE;
        }
        else if (eventItems.length == 5)
        {
            // Event ID for DML.
            this.commitSCN = parseLong(eventItems[0]);
            xid = eventItems[1];
            changeSeq = eventItems[2];
            startSCN = parseLong(eventItems[3]);
            startPlog = parseInt(eventItems[4]);
        }
        else if (eventItems.length == 6)
        {
            // Event ID for DDL that includes # in the XID.
            commitSCN = parseLong(eventItems[0]);
            xid = "#" + eventItems[2];
            changeSeq = eventItems[3];
            startSCN = parseLong(eventItems[4]);
            startPlog = parseInt(eventItems[5]);
        }
        else
        {
            // This is an unrecognized Event ID.
            throw new ReplicatorException("Malformed eventId: " + eventId);
        }
    }

    public String getEventId()
    {
        return eventId;
    }

    public long getCommitSCN()
    {
        return commitSCN;
    }

    public String getXid()
    {
        return xid;
    }

    public String getChangeSeq()
    {
        return changeSeq;
    }

    public long getStartSCN()
    {
        return startSCN;
    }

    public int getStartPlog()
    {
        return startPlog;
    }

    /**
     * Parse a long, catching errors.
     */
    private long parseLong(String longAsString) throws ReplicatorException
    {
        try
        {
            return Long.parseLong(longAsString);
        }
        catch (NumberFormatException e)
        {
            throw new ReplicatorException(
                    "Malformed event ID; unable to parse number value: eventId="
                            + eventId + " number=" + longAsString);
        }
    }

    /**
     * Parse an integer, catching errors.
     */
    private int parseInt(String intAsString) throws ReplicatorException
    {
        try
        {
            return Integer.parseInt(intAsString);
        }
        catch (NumberFormatException e)
        {
            throw new ReplicatorException(
                    "Malformed event ID; unable to parse number value: eventId="
                            + eventId + " number=" + intAsString);
        }
    }
}
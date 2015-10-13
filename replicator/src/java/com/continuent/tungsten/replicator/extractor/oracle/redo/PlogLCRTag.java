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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.sql.Timestamp;
import java.util.GregorianCalendar;

/**
 * Represents one tag in an LCR entry and the data contained within.
 */
class PlogLCRTag implements Serializable
{
    private static final long serialVersionUID = 1L;
    int[] rawData;
    int   length;
    int   id;
    short prchar; /*
                   * set by typeAsString; shows if value dump should try to
                   * print ASCII, too; -1 = no, >=0 : start offset
                   */

    /* tags */
    /* 0: general */
    static final int TAG_FILESIZE              = 0x00000001; /*
                                                              * size of plog
                                                              * file
                                                              */
    static final int TAG_FILE_CHECKSUM         = 0x00000002; /*
                                                              * checksum of plog
                                                              */
    static final int TAG_FOOTER_START          = 0x00000003; /*
                                                              * start of footer
                                                              * entry, counted
                                                              * from
                                                              * end-of-plog-file
                                                              */
    static final int TAG_CHECKSUM_ALG          = 0x00000004; /*
                                                              * define algorithm
                                                              * used for
                                                              * checksums
                                                              */
    static final int TAG_ALG_CRC32B            = 0x1;
    static final int TAG_FOOTER_COMMENT        = 0x00000005; /*
                                                              * start of footer
                                                              * entry, counted
                                                              * from
                                                              * end-of-plog-file
                                                              */
    static final int TAG_MINE_UUID             = 0x00000006; /*
                                                              * unique id of
                                                              * mine
                                                              */
    static final int TAG_PLOGSEQ               = 0x00000007; /*
                                                              * plog seq 1 int
                                                              */
    static final int TAG_FEATURES_1            = 0x00000008; /*
                                                              * bitfields of
                                                              * enabled features
                                                              */
    static final int TAG_FEATURES_1_CACHE_DICT = 0x00000001; /*
                                                              * plog compression
                                                              * - do not put any
                                                              * column name/type
                                                              * twice into the
                                                              * same plog
                                                              */
    static final int TAG_CHECKSUM              = 0x00000009; /*
                                                              * entry checksum
                                                              */
    static final int TAG_PLOGNAME              = 0x00000010; /* named plog */
    static final int TAG_PLOG_FILENAME         = 0x00000011; /* named plog */
    static final int TAG_APPLY_NAME            = 0x00000012; /*
                                                              * named of apply
                                                              * that should
                                                              * parse this
                                                              */
    static final int TAG_ROW_COUNT             = 0x00000013; /*
                                                              * number of rows,
                                                              * e.g. in an IFILE
                                                              */

    /* 1: database info */
    static final int TAG_DBID      = 0x00010001; /* database id - 1 int */
    static final int TAG_THREADSEQ = 0x00010002; /* thread and seq# - 2 int */
    static final int TAG_LOWSCN    = 0x00010003; /* first scn */
    static final int TAG_HIGHSCN   = 0x00010004; /* last scn */

    /* 2: column data */
    static final int TAG_PREIMAGE               = 0x00020001; /*
                                                               * old column
                                                               * values - 1 byte
                                                               * (length in
                                                               * bytes) + 4-byte
                                                               * padded column
                                                               * data)
                                                               */
    static final int TAG_POSTIMAGE              = 0x00020002; /*
                                                               * new column
                                                               * values - 1 byte
                                                               * (length in
                                                               * bytes) + 4-byte
                                                               * padded column
                                                               * data)
                                                               */
    static final int TAG_KEYIMAGE               = 0x00020003; /*
                                                               * key
                                                               * (supplemental
                                                               * logging) column
                                                               * values - 1 byte
                                                               * (length in
                                                               * bytes) + 4-byte
                                                               * padded column
                                                               * data)
                                                               */
    static final int TAG_LOBDATA                = 0x00020004; /*
                                                               * lob data -
                                                               * uint64_t length
                                                               * + 4-byte padded
                                                               * actual data
                                                               */
    static final int TAG_LOBOFFSET              = 0x00020005; /*
                                                               * uint64_t lob
                                                               * offset - where
                                                               * to start
                                                               */
    static final int TAG_LOB_POSITION           = 0x00020006; /*
                                                               * uint32_t, see
                                                               * below
                                                               */
    static final int TAG_CDC                    = 0x00020007; /*
                                                               * CDC column; not
                                                               * used in plog,
                                                               * only internally
                                                               * in apply
                                                               */
    static final int LOB_ONE_PIECE              = 1;
    static final int LOB_FIRST_PIECE            = 2;
    static final int LOB_NEXT_PIECE             = 3;
    static final int LOB_LAST_PIECE             = 4;
    static final int TAG_LOBLEN                 = 0x00020007; /*
                                                               * uint64_t lob
                                                               * offset - end
                                                               */
    static final int TAG_LOBLOCATOR             = 0x00020008; /*
                                                               * 10 bytes
                                                               * locator
                                                               */
    static final int TAG_COL_ID                 = 0x00020010; /* column id */
    static final int TAG_COL_NAME               = 0x00020011; /* column name */
    static final int TAG_COL_TYPE               = 0x00020012; /*
                                                               * column type
                                                               * TBD: how to
                                                               * encode
                                                               * datatypes
                                                               */
    static final int TAG_OBJ_ID                 = 0x00020020; /* object id */
    static final int TAG_OBJ_NAME               = 0x00020021; /* object name */
    static final int TAG_OBJ_OWNER              = 0x00020022; /* object owner */
    static final int TAG_BASEOBJ_ID             = 0x00020023; /*
                                                               * base object id
                                                               */
    static final int TAG_LCR_ID                 = 0x00020030; /*
                                                               * LCR sequence id
                                                               */
    static final int TAG_SAVEPOINT_ID           = 0x00020031; /*
                                                               * LCR sequence id
                                                               * + 1e6*plog_id
                                                               * (for rollback
                                                               * to savepoint)
                                                               */
    static final int TAG_RBA                    = 0x00020032; /*
                                                               * Redo Block
                                                               * Address
                                                               */
    static final int TAG_COLUMN_SIGNATURE_PRE   = 0x00200101; /*
                                                               * length 1 int +
                                                               * 4-byte padded -
                                                               * describes used
                                                               * columns and
                                                               * null/not null
                                                               */
    static final int TAG_COLUMN_SIGNATURE_POST  = 0x00200102; /*
                                                               * length 1 int +
                                                               * 4-byte padded
                                                               */
    static final int TAG_COLUMN_SIGNATURE_KEY   = 0x00200103; /*
                                                               * length 1 int +
                                                               * 4-byte padded
                                                               */
    static final int SIGNATURE_HAS_DATA         = 0x1000;     // lower 10 bits
                                                              // is column
                                                              // number
    static final int SIGNATURE_NOT_PRESENT      = 0x000;
    static final int SIGNATURE_NULL             = 0x2000;
    static final int SIGNATURE_HAS_DATA_OR_NULL = 0x3000;     /*
                                                               * for new values
                                                               * does not matter
                                                               */

    /* 3: transaction management */
    static final int TAG_XID           = 0x00030001; /*
                                                      * transaction id of
                                                      * current transaction
                                                      */
    static final int TAG_SCN           = 0x00030002; /*
                                                      * SCN of current operation
                                                      */
    static final int TAG_DTIME         = 0x00030003; /*
                                                      * time of current
                                                      * operation
                                                      */
    static final int TAG_AUDIT_SID     = 0x00030010; /* session ID */
    static final int TAG_AUDIT_SERIAL  = 0x00030011; /* session serial# */
    static final int TAG_AUDIT_CUSER   = 0x00030012; /* current username */
    static final int TAG_AUDIT_LUSER   = 0x00030013; /* logon username */
    static final int TAG_AUDIT_CLIINFO = 0x00030014; /* client information */
    static final int TAG_AUDIT_OSUSER  = 0x00030015; /* OS username */
    static final int TAG_AUDIT_MACHINE = 0x00030016; /* machine name */
    static final int TAG_AUDIT_OSTERM  = 0x00030017; /* OS terminal */
    static final int TAG_AUDIT_OSPROC  = 0x00030018; /* OS process id */
    static final int TAG_AUDIT_OSPROG  = 0x00030019; /* OS program name */
    static final int TAG_XID_NAME      = 0x0003001a; /*
                                                      * transaction id of
                                                      * current transaction
                                                      */

    /* x10: DDL */
    static final int TAG_LOGON_SCHEMA   = 0x0040001; /*
                                                      * zero terminated strings
                                                      */
    static final int TAG_CURRENT_SCHEMA = 0x0040002;
    static final int TAG_SQL_TEXT       = 0x0040003;
    static final int TAG_DDL_SQLOP      = 0x0040004; /* integer */

    /**
     * Human readable text description of type and subtype. Also sets prchar
     * indicating if the actual data can be converted to text and shown to a
     * human.
     * 
     * @return string with text
     */
    public String typeAsString()
    {
        String typeStr = "?";
        switch (this.id)
        {
            case TAG_CHECKSUM :
                typeStr = " (entry checksum)";
                prchar = -1;
                break;
            case TAG_FILESIZE :
                typeStr = " (plog file size)";
                prchar = -1;
                break;
            case TAG_FILE_CHECKSUM :
                typeStr = " (plog file checksum)";
                prchar = -1;
                break;
            case TAG_FOOTER_START :
                typeStr = " (footer entry start)";
                prchar = -1;
                break;
            case TAG_FOOTER_COMMENT :
                typeStr = " (footer comment)";
                prchar = -1;
                break;
            case TAG_CHECKSUM_ALG :
                typeStr = " (checksum algorithm)";
                prchar = -1;
                break;
            case TAG_MINE_UUID :
                typeStr = " (mine UUID)";
                prchar = 0;
                break;
            case TAG_PLOGSEQ :
                typeStr = " (plog sequence)";
                prchar = -1;
                break;
            case TAG_FEATURES_1 :
                typeStr = " (plog features #1)";
                prchar = -1;
                break;
            case TAG_PLOGNAME :
                typeStr = " (plog name)";
                prchar = 0;
                break;
            case TAG_PLOG_FILENAME :
                typeStr = " (plog file name)";
                prchar = 0;
                break;
            case TAG_APPLY_NAME :
                typeStr = " (apply name)";
                prchar = 0;
                break;
            case TAG_ROW_COUNT :
                typeStr = " (row count)";
                prchar = -1;
                break;

            case TAG_DBID :
                typeStr = " (DBID)";
                prchar = -1;
                break;
            case TAG_THREADSEQ :
                typeStr = " (thread and seq#)";
                prchar = -1;
                break;
            case TAG_LOWSCN :
                typeStr = " (first SCN)";
                prchar = -1;
                break;
            case TAG_HIGHSCN :
                typeStr = " (last SCN)";
                prchar = -1;
                break;

            case TAG_PREIMAGE :
                typeStr = " (column old value)";
                prchar = 1;
                break;
            case TAG_POSTIMAGE :
                typeStr = " (column new value)";
                prchar = 1;
                break;
            case TAG_KEYIMAGE :
                typeStr = " (column supplemental logging value)";
                prchar = 1;
                break;
            case TAG_LOBDATA :
                typeStr = " (LOB data)";
                prchar = 2;
                break;
            case TAG_COL_ID :
                typeStr = " (column id)";
                prchar = -1;
                break;
            case TAG_COL_NAME :
                typeStr = " (column name)";
                prchar = 0;
                break;
            case TAG_COL_TYPE :
                typeStr = " (column type)";
                prchar = 0;
                break;
            case TAG_OBJ_ID :
                typeStr = " (object id)";
                prchar = -1;
                break;
            case TAG_OBJ_NAME :
                typeStr = " (object name)";
                prchar = 0;
                break;
            case TAG_OBJ_OWNER :
                typeStr = " (object owner)";
                prchar = 0;
                break;
            case TAG_LCR_ID :
                typeStr = " (LCR sequence id)";
                prchar = 0;
                break;
            case TAG_SAVEPOINT_ID :
                typeStr = " (savepoint id)";
                prchar = 0;
                break;
            case TAG_RBA :
                typeStr = " (RBA)";
                prchar = 0;
                break;
            case TAG_COLUMN_SIGNATURE_PRE :
                typeStr = " (column signature pre)";
                prchar = -1;
                break;
            case TAG_COLUMN_SIGNATURE_POST :
                typeStr = " (column signature post)";
                prchar = -1;
                break;
            case TAG_COLUMN_SIGNATURE_KEY :
                typeStr = " (column signature key)";
                prchar = -1;
                break;
            case TAG_LOBOFFSET :
                typeStr = " (LOB offset)";
                prchar = -1;
                break;
            case TAG_LOB_POSITION :
                typeStr = " (LOB position one/first/next/last)";
                prchar = -1;
                break;
            case TAG_LOBLEN :
                typeStr = " (LOB length)";
                prchar = -1;
                break;
            case TAG_LOBLOCATOR :
                typeStr = " (LOB locator)";
                prchar = -1;
                break;

            case TAG_XID :
                typeStr = " (transaction id)";
                prchar = 0;
                break;
            case TAG_SCN :
                typeStr = " (SCN)";
                prchar = 0;
                break;
            case TAG_DTIME :
                typeStr = " (datetime)";
                prchar = -1;
                break;
            case TAG_AUDIT_SID :
                typeStr = " (SID)";
                prchar = 0;
                break;
            case TAG_AUDIT_SERIAL :
                typeStr = " (serial#)";
                prchar = 0;
                break;
            case TAG_AUDIT_CUSER :
                typeStr = " (current user)";
                prchar = 0;
                break;
            case TAG_AUDIT_LUSER :
                typeStr = " (logon user)";
                prchar = 0;
                break;
            case TAG_AUDIT_CLIINFO :
                typeStr = " (client info)";
                prchar = 0;
                break;
            case TAG_AUDIT_OSUSER :
                typeStr = " (OS user)";
                prchar = 0;
                break;
            case TAG_AUDIT_MACHINE :
                typeStr = " (machine name)";
                prchar = 0;
                break;
            case TAG_AUDIT_OSTERM :
                typeStr = " (OS terminal)";
                prchar = 0;
                break;
            case TAG_AUDIT_OSPROC :
                typeStr = " (OS process id)";
                prchar = 0;
                break;
            case TAG_AUDIT_OSPROG :
                typeStr = " (OS program name)";
                prchar = 0;
                break;
            case TAG_XID_NAME :
                typeStr = " (transaction name)";
                prchar = 0;
                break;

            case TAG_LOGON_SCHEMA :
                typeStr = " (logon schema)";
                prchar = 0;
                break;
            case TAG_CURRENT_SCHEMA :
                typeStr = " (current schema)";
                prchar = 0;
                break;
            case TAG_SQL_TEXT :
                typeStr = " (SQL text)";
                prchar = 0;
                break;
            case TAG_DDL_SQLOP :
                typeStr = " (DDL operation)";
                prchar = -1;
                break;
            default :
                typeStr = " (???)";
                prchar = -1;
                break;
        }
        return typeStr;
    }

    /**
     * Convert tag value to a string. For now it assumes US-ASCII and thus does
     * not support non-ASCII in column/table names. See RQ-1881
     * 
     * @return string with text
     */
    public String valueString()
    {
        byte[] barr = new byte[rawData.length * 4];
        ByteBuffer b = ByteBuffer.wrap(barr, 0, barr.length);
        b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        IntBuffer i = b.asIntBuffer();
        i.put(rawData);
        try
        {
            String str = new String(barr,
                    "US-ASCII"); /* FIXME handle encoding (see also RQ-1881) */
            str = str.substring(0,str.indexOf("\0"));  // strip of trailing NULL and anything further
            return str;
        }
        catch (java.io.UnsupportedEncodingException e)
        {
            // FIXME
        }
        return null;
    }

    /**
     * Convert tag value to a long (64-bit).
     * 
     * @return long value
     */
    public long valueLong()
    {
        byte[] barr = new byte[rawData.length * 4];
        ByteBuffer b = ByteBuffer.wrap(barr, 0, barr.length);
        b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        IntBuffer i = b.asIntBuffer();
        i.put(0, rawData[0]);
        i.put(1, rawData[1]);
        LongBuffer l = b.asLongBuffer();
        return l.get();
    }

    /**
     * Convert tag value to an int (32-bit).
     * 
     * @return int value
     */
    public int valueInt()
    {
        return rawData[0];
    }

    /**
     * Convert tag value to a timestamp. Source is Oracle internal time
     * representation as used in redo headers etc.
     * 
     * @return Java Timestamp value
     */
    public Timestamp valueDtime()
    {
        int t, seconds, minutes, hours, day, month, year;
        t = rawData[0];
        seconds = t % 60;
        t /= 60;
        minutes = t % 60;
        t /= 60;
        hours = t % 24;
        t /= 24;
        day = t % 31 + 1;
        t /= 31;
        month = t % 12 + 1;
        t /= 12;
        year = t + 1988;
        GregorianCalendar gc = new GregorianCalendar(year, month-1 /*Java starts with month 0*/, day, hours,
                minutes, seconds);
        return new Timestamp(gc.getTimeInMillis());
    }
}
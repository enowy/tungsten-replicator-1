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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;

import javax.sql.rowset.serial.SerialException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.AdditionalTypes;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Represents one logical change record (=one entry in the plog).
 */
class PlogLCR implements Serializable
{
    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger.getLogger(PlogLCR.class);

    LinkedList<PlogLCRTag> rawTags;
    public int             length;
    public int             type;
    public int             subtype;

    /* Replicator event Id (set on transaction commit) */
    public String eventId;

    // Oracle transaction ID
    public String             XID;
    // unique id of LCR
    public long               LCRid;
    // if ROLLBACK TO SAVEPOINT: LCR id where to roll back
    public long               LCRSavepointId;
    public long               SCN;
    public java.sql.Timestamp timestamp;
    public String             tableOwner   = "";
    public String             tableName    = "";
    public int                tableId      = -1;
    ArrayList<oneColVal>      columnValues = new ArrayList<oneColVal>();

    public String SQLText       = "";
    public String currentSchema = "";

    /* header, footer, ... */
    static final int ETYPE_CONTROL = 0;

    /* file header */
    static final int ESTYPE_HEADER                     = 0;
    /* file footer */
    static final int ESTYPE_FOOTER                     = 1;
    /* start transaction, commit */
    static final int ETYPE_TRANSACTIONS                = 1;
    /* start transaction */
    static final int ESTYPE_TRAN_START                 = 1;
    /* commit transaction */
    static final int ESTYPE_TRAN_COMMIT                = 4;
    /* rollback transaction */
    static final int ESTYPE_TRAN_ROLLBACK              = 5;
    /* rollback part of transaction */
    static final int ESTYPE_TRAN_ROLLBACK_TO_SAVEPOINT = 6;
    /* rollback part of transaction */
    static final int ESTYPE_TRAN_AUDIT                 = 7;

    /*
     * LCR denote all transaction-related changes, including start transaction
     * and commit; LCR_DATA means real data change
     */
    /* logical change record (real data) */
    static final int ETYPE_LCR_DATA       = 11;
    /* insert */
    static final int ESTYPE_LCR_INSERT    = 2;
    /* delete */
    static final int ESTYPE_LCR_DELETE    = 3;
    /* update */
    static final int ESTYPE_LCR_UPDATE    = 5;
    /* LOB write */
    static final int ESTYPE_LCR_LOB_WRITE = 0x10;
    /* LOB trim */
    static final int ESTYPE_LCR_LOB_TRIM  = 0x11;
    /* LOB erase */
    static final int ESTYPE_LCR_LOB_ERASE = 0x12;
    /* actual DDL text */
    static final int ESTYPE_LCR_DDL       = 0x100;

    static final int ETYPE_LCR_PLOG              = 20;
    // load file
    static final int ESTYPE_LCR_PLOG_IFILE       = 1;
    // number of rows in the IFILE just loaded
    static final int ESTYPE_LCR_PLOG_IFILE_STATS = 2;

    /**
     * Represents one column and it's value. Used (and passed around) when
     * building the resulting DBMSEvent. This class is public just so that other
     * classes can make new instances of it.
     */
    public class oneColVal
    {
        public int    imageType;
        public String name;
        public String datatype;
        public int    id;
        public int[]  rawVal;
        public long   lobOffset;
        public long   lobLength;
        public int    lobPosition;

        private boolean alreadyParsedValue = false;

        public ColumnSpec columnSpec;
        public ColumnVal  columnVal;

        public int typeAsSQLType() throws ReplicatorException
        {
            if (this.datatype.equals(("NUMBER")))
            {
                return java.sql.Types.NUMERIC;
            }
            else
                if (this.datatype.equals("VARCHAR2")
                        || this.datatype.equals("VARCHAR")
                        || this.datatype.equals("CHAR")
                        || this.datatype.equals("NVARCHAR2")
                        || this.datatype.equals("NVARCHAR")
                        || this.datatype.equals("NCHAR")
                        || this.datatype.equals("LONG"))
            {
                return java.sql.Types.VARCHAR;
            }
            else if (this.datatype.equals("DATE"))
            {
                return java.sql.Types.DATE;
            }
            else if (this.datatype.equals("TIMESTAMP"))
            {
                return java.sql.Types.TIMESTAMP;
            }
            else if (this.datatype.equals("TIMESTAMP WITH TIME ZONE"))
            {
                return java.sql.Types.TIMESTAMP;
            }
            else if (this.datatype.equals("TIMESTAMP WITH LOCAL TIME ZONE"))
            {
                return java.sql.Types.TIMESTAMP;
            }
            else if (this.datatype.equals("INTERVAL DAY TO SECOND"))
            {
                return AdditionalTypes.INTERVALDS;
            }
            else if (this.datatype.equals("INTERVAL YEAR TO MONTH"))
            {
                return AdditionalTypes.INTERVALYM;
            }
            else
                if (this.datatype.equals("CLOB")
                        || this.datatype.equals("CLOB_UTF16")
                        || this.datatype.equals("NCLOB"))
            {
                return java.sql.Types.VARCHAR;
            }
            else
                    if (this.datatype.equals("BLOB")
                            || this.datatype.equals("RAW")
                            || this.datatype.equals("LONG RAW"))
            {
                return java.sql.Types.BLOB;
            }
            else
            {
                logger.error(this.datatype + " is currently not supported.");
                return java.sql.Types.VARCHAR;
            }

        }

        /**
         * Parse a column value from Oracle internal representation into Java
         * type.
         * 
         * @throws UnsupportedEncodingException, ReplicatorException,
         *             SerialException, SQLException
         */
        public void parseValue() throws UnsupportedEncodingException,
                ReplicatorException, SQLException
        {

            if (alreadyParsedValue)
                return;
            /*
             * no raw data present, treat as NULL. Happens when LOB update with
             * actual data is coming next
             */
            if (rawVal == null)
            {
                imageType = PlogLCRTag.TAG_POSTIMAGE;
                columnVal.setValueNull();
                columnSpec.setLength(0);
                alreadyParsedValue = true;
                return; /* no data = Oracle NULL */
            }
            byte[] barr = new byte[rawVal.length * 4];
            ByteBuffer b = ByteBuffer.wrap(barr, 0, barr.length);
            b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            IntBuffer i = b.asIntBuffer();
            i.put(rawVal);
            /* first int is value length; data start at barr[4] */
            int rawLength = b.getInt();
            final int dataoff = 4;

            if (rawLength == 0)
            {
                columnVal.setValueNull();
                columnSpec.setLength(0);
                alreadyParsedValue = true;
                rawVal = null;
                return; /* no data = Oracle NULL */
            }

            if (this.datatype.equals(("NUMBER")))
            {
                if (rawLength == 1 && ((int) barr[dataoff + 0] & 0xff) == 0x80)
                { /* "80" is 0 */
                    columnVal.setValue(new BigDecimal(0));
                    columnSpec.setLength(1);
                }
                else
                {
                    int length;
                    int shift;
                    StringBuilder rtval = new StringBuilder();
                    if (barr[dataoff + rawLength - 1] == 0x66)
                    { /* last byte 0x66 = negative number */
                        length = rawLength - 1;
                        /*
                         * 1 = /100, -1=*100 (reference decimal point: after
                         * first digit pair)
                         */
                        shift = -(((int) barr[dataoff + 0] & 0xff) - 0x3e);
                        rtval.append("-.");
                        for (int o = 1; o < length; o++)
                            rtval.append(String.format("%02d",
                                    101 - ((int) barr[dataoff + o] & 0xff)));
                    }
                    else
                    {
                        length = rawLength;
                        rtval.append(".");
                        shift = ((int) barr[dataoff + 0] & 0xff) - 0xc1; // the
                                                                         // (int)&0xff
                                                                         // is
                                                                         // to
                                                                         // force
                                                                         // unsigned
                                                                         // treatment
                        for (int o = 1; o < length; o++)
                            rtval.append(String.format("%02d",
                                    ((int) barr[dataoff + o] & 0xff) - 1));
                    }
                    BigDecimal bd = new BigDecimal(rtval.toString());
                    // +2 because we put . before all digits; but Oracle format
                    // puts if after first pair. +2 in scale offsets this. Also
                    // we recreate the big decimal to avoid triggering
                    // scientific notation on printing.
                    bd = bd.scaleByPowerOfTen(shift * 2 + 2);
                    String bdPlainString = bd.toPlainString();
                    BigDecimal bd2Rescaled = new BigDecimal(bdPlainString);
                    columnVal.setValue(bd2Rescaled);
                    columnSpec.setLength(bdPlainString.length());
                }
            }
            else
                if (this.datatype.equals("VARCHAR2")
                        || this.datatype.equals("VARCHAR")
                        || this.datatype.equals("CHAR")
                        || this.datatype.equals("NVARCHAR2")
                        || this.datatype.equals("NVARCHAR")
                        || this.datatype.equals("NCHAR")
                        || this.datatype.equals("LONG"))
            {
                byte[] trimmed = new byte[rawLength];
                b.get(trimmed, 0, rawLength);
                String parsedVal = new String(trimmed, "UTF-8");
                columnVal.setValue(parsedVal);
                columnSpec.setLength(parsedVal.length());
            }
            else
                    if (this.datatype.equals("RAW")
                            || this.datatype.equals("LONG RAW"))
            {
                byte[] trimmed = new byte[rawLength];
                b.get(trimmed, 0, rawLength);
                SerialBlob parsedVal = new SerialBlob(trimmed);
                columnVal.setValue(parsedVal);
                columnSpec.setLength((int) parsedVal.length());
            }
            else if (this.datatype.equals("DATE"))
            {
                // 0: 100-offset centry; 1: 100-offset year
                GregorianCalendar gcal = new GregorianCalendar(
                        (((int) barr[dataoff + 0] & 0xff) - 100) * 100
                                + (((int) barr[dataoff + 1] & 0xff) - 100),
                        barr[dataoff + 2] - 1, // 0-offset month, but -1 for
                                               // Java (January is 0)
                        barr[dataoff + 3], // 0-offset day
                        barr[dataoff + 4] - 1, // 1-offset hour
                        barr[dataoff + 5] - 1, // 1-offset minute
                        barr[dataoff + 6] - 1 // 1-offset second
                );
                java.sql.Timestamp ts = new java.sql.Timestamp(
                        gcal.getTimeInMillis());
                columnVal.setValue(ts);
                columnSpec.setLength(ts.toString().length());
            }
            else if (this.datatype.equals("TIMESTAMP"))
            {
                GregorianCalendar gcal = new GregorianCalendar(
                        (((int) barr[dataoff + 0] & 0xff) - 100) * 100
                                + (((int) barr[dataoff + 1] & 0xff) - 100), // 0:
                                                                            // 100-offset
                                                                            // centry;
                                                                            // 1:
                                                                            // 100-offset
                                                                            // year
                        barr[dataoff + 2] - 1, // 0-offset month, but -1 for
                                               // Java (January is 0)
                        barr[dataoff + 3], // 0-offset day
                        barr[dataoff + 4] - 1, // 1-offset hour
                        barr[dataoff + 5] - 1, // 1-offset minute
                        barr[dataoff + 6] - 1 // 1-offset second
                );
                int umilli = 0;
                for (int j = 7; j < rawLength; j++)
                    umilli = (umilli << 8) + ((int) barr[dataoff + j] & 0xff);
                java.sql.Timestamp ts = new java.sql.Timestamp(
                        gcal.getTimeInMillis());
                ts.setNanos(umilli);
                columnVal.setValue(ts);
                columnSpec.setLength(ts.toString().length());
                /*
                 * WARNING: Calendar does not support enough precision!!!;
                 * java.sql.timestamp does not have time zone information, we
                 * expect to have always UTC from mine (MINE_CONVERT_TZ_TO_UTC =
                 * YES)
                 */
            }
            else if (this.datatype.equals("TIMESTAMP WITH TIME ZONE"))
            {
                GregorianCalendar gcal = new GregorianCalendar(
                        (((int) barr[dataoff + 0] & 0xff) - 100) * 100
                                + (((int) barr[dataoff + 1] & 0xff) - 100), // 0:
                                                                            // 100-offset
                                                                            // centry;
                                                                            // 1:
                                                                            // 100-offset
                                                                            // year
                        barr[dataoff + 2] - 1, // 0-offset month, but -1 for
                                               // Java (January is 0)
                        barr[dataoff + 3], // 0-offset day
                        barr[dataoff + 4] - 1, // 1-offset hour
                        barr[dataoff + 5] - 1, // 1-offset minute
                        barr[dataoff + 6] - 1 // 1-offset second
                );
                int umilli = 0;
                for (int j = 7; j < rawLength - 2; j++)
                    umilli = (umilli << 8) + ((int) barr[dataoff + j] & 0xff);
                int tzh = ((int) barr[dataoff + rawLength - 2] & 0xff);
                int tzm = ((int) barr[dataoff + rawLength - 1] & 0xff);
                if (tzh != 0xd0 || tzm != 0x4)
                {
                    logger.warn(
                            "Timezone was not UTC in TIMESTAMP WITH TIME ZONE (tzh="
                                    + tzh + ", tzm=" + tzm + ")");
                }
                java.sql.Timestamp ts = new java.sql.Timestamp(
                        gcal.getTimeInMillis());
                ts.setNanos(umilli);
                columnVal.setValue(ts);
                columnSpec.setLength(ts.toString().length());
            }
            else if (this.datatype.equals("TIMESTAMP WITH LOCAL TIME ZONE"))
            {
                GregorianCalendar gcal = new GregorianCalendar(
                        (((int) barr[dataoff + 0] & 0xff) - 100) * 100
                                + (((int) barr[dataoff + 1] & 0xff) - 100), // 0:
                                                                            // 100-offset
                                                                            // centry;
                                                                            // 1:
                                                                            // 100-offset
                                                                            // year
                        barr[dataoff + 2] - 1, // 0-offset month, but -1 for
                                               // Java (January is 0)
                        barr[dataoff + 3], // 0-offset day
                        barr[dataoff + 4] - 1, // 1-offset hour
                        barr[dataoff + 5] - 1, // 1-offset minute
                        barr[dataoff + 6] - 1 // 1-offset second
                );
                int umilli = 0;
                for (int j = 7; j < rawLength; j++)
                    umilli = (umilli << 8) + ((int) barr[dataoff + j] & 0xff);
                java.sql.Timestamp ts = new java.sql.Timestamp(
                        gcal.getTimeInMillis());
                ts.setNanos(umilli);
                columnVal.setValue(ts);
                columnSpec.setLength(ts.toString().length());
            }
            else if (this.datatype.equals("INTERVAL DAY TO SECOND"))
            {
                long d = 0;
                for (int j = 0; j < 4; j++)
                    d = (d << 8) + ((int) barr[dataoff + j] & 0xff);
                d -= 0x80000000L;
                int h = ((int) barr[dataoff + 4] & 0xff) - 60;
                int mi = ((int) barr[dataoff + 5] & 0xff) - 60;
                int s = ((int) barr[dataoff + 6] & 0xff) - 60;
                long umilli = 0;
                for (int j = 7; j < 11; j++)
                    umilli = (umilli << 8) + ((int) barr[dataoff + j] & 0xff);
                umilli -= 0x80000000L;
                String parsedVal = String.format("%+d %d:%d:%d.%05d", d,
                        Math.abs(h), Math.abs(mi), Math.abs(s),
                        Math.abs(umilli));
                columnVal.setValue(parsedVal);
                columnSpec.setLength(parsedVal.length());
            }
            else if (this.datatype.equals("INTERVAL YEAR TO MONTH"))
            {
                long y = 0;
                for (int j = 0; j < 4; j++)
                    y = (y << 8) + ((int) barr[dataoff + j] & 0xff);
                y -= 0x80000000L;
                int m = ((int) barr[dataoff + 4] & 0xff) - 60;
                String parsedVal = String.format("%+d-%d", y, Math.abs(m));
                columnVal.setValue(parsedVal);
                columnSpec.setLength(parsedVal.length());
            }
            else
                if (this.datatype.equals("CLOB")
                        || this.datatype.equals("CLOB_UTF16")
                        || this.datatype.equals("NCLOB"))
            {
                /*
                 * upper 32-bit of length... ignore for now, we handle chunks
                 * <4GB only
                 */
                @SuppressWarnings("unused")
                int rawLengthUpper32 = b.getInt();
                int actualLength;
                if (this.datatype.equals("CLOB_UTF16")
                        || this.datatype.equals("NCLOB"))
                {
                    /* lobLength is in bytes - convert to chars */
                    actualLength = lobLength > 0
                            ? (int) lobLength / 2
                            : rawLength;
                }
                else
                { /* CLOB = 8-bit on source */
                    actualLength = lobLength > 0 ? (int) lobLength : rawLength;
                }
                if (logger.isDebugEnabled())
                {
                    logger.debug(bytesToHex(barr));
                    logger.debug("CLOB:" + actualLength + "(lobLength="
                            + lobLength + ", rawLength=" + rawLength + ")");
                }
                byte[] trimmed = new byte[rawLength];
                b.get(trimmed, 0, rawLength);
                String parsedVal = new String(trimmed, "UTF-8").substring(0,
                        actualLength);
                columnVal.setValue(parsedVal);
                columnSpec.setLength(parsedVal.length());
            }
            else if (this.datatype.equals("BLOB"))
            {
                /*
                 * upper 32-bit of length... ignore for now, we handle chunks
                 * <4GB only
                 */
                @SuppressWarnings("unused")
                int rawLengthUpper32 = b.getInt();
                int actualLength = lobLength > 0 ? (int) lobLength : rawLength;
                if (logger.isDebugEnabled())
                {
                    logger.debug(bytesToHex(barr));
                    logger.debug("BLOB:" + actualLength + "(lobLength="
                            + lobLength + ", rawLength=" + rawLength);
                }
                byte[] trimmed = new byte[actualLength];
                b.get(trimmed, 0, actualLength);
                SerialBlob parsedVal = new SerialBlob(trimmed);
                columnVal.setValue(parsedVal);
                columnSpec.setLength((int) parsedVal.length());
            }
            else
            {
                columnVal.setValueNull();
                columnSpec.setLength(0);
                alreadyParsedValue = true;
                // throw new ReplicatorException(this.datatype + " is currently
                // not supported.");
            }
            alreadyParsedValue = true;
            rawVal = null;
        }
    }

    /**
     * Human readable text description of type and subtype
     */
    public String typeAsString()
    {
        String typeStr = "?";
        String subtypeStr = "?";

        switch (this.type)
        {
            case ETYPE_CONTROL :
                typeStr = " control/";
                switch (this.subtype)
                {
                    case ESTYPE_HEADER :
                        subtypeStr = "header";
                        break;
                    case ESTYPE_FOOTER :
                        subtypeStr = "footer";
                        break;
                }
                break;
            case ETYPE_TRANSACTIONS :
                typeStr = " transaction/";
                switch (this.subtype)
                {
                    case ESTYPE_TRAN_START :
                        subtypeStr = "start";
                        break;
                    case ESTYPE_TRAN_COMMIT :
                        subtypeStr = "commit";
                        break;
                    case ESTYPE_TRAN_ROLLBACK :
                        subtypeStr = "rollback";
                        break;
                    case ESTYPE_TRAN_ROLLBACK_TO_SAVEPOINT :
                        subtypeStr = "rollback_to_savepoint";
                        break;
                    case ESTYPE_TRAN_AUDIT :
                        subtypeStr = "audit";
                        break;
                }
                break;
            case ETYPE_LCR_DATA :
                typeStr = " LCR data/";
                switch (this.subtype)
                {
                    case ESTYPE_LCR_INSERT :
                        subtypeStr = "insert";
                        break;
                    case ESTYPE_LCR_DELETE :
                        subtypeStr = "delete";
                        break;
                    case ESTYPE_LCR_UPDATE :
                        subtypeStr = "update";
                        break;
                    case ESTYPE_LCR_LOB_WRITE :
                        subtypeStr = "LOB write";
                        break;
                    case ESTYPE_LCR_LOB_TRIM :
                        subtypeStr = "LOB trim";
                        break;
                    case ESTYPE_LCR_LOB_ERASE :
                        subtypeStr = "LOB erase";
                        break;
                    case ESTYPE_LCR_DDL :
                        subtypeStr = "DDL";
                        break;
                }
                break;
            case ETYPE_LCR_PLOG :
                typeStr = " LCR plog/";
                switch (this.subtype)
                {
                    case ESTYPE_LCR_PLOG_IFILE :
                        subtypeStr = "include plog file";
                        break;
                    case ESTYPE_LCR_PLOG_IFILE_STATS :
                        subtypeStr = "include plog file - rowcount";
                        break;
                }
                break;
            default :
                typeStr = " ?" + this.type + "/";
                subtypeStr = "?" + this.subtype;
        }
        return typeStr + subtypeStr;
    }

    /**
     * Parse data for a DDL change
     */
    public void parseDDLInfo()
    {
        for (PlogLCRTag tag : rawTags)
        {
            switch (tag.id)
            {
                case PlogLCRTag.TAG_SQL_TEXT :
                    this.SQLText = tag.valueString();
                    break;
                case PlogLCRTag.TAG_CURRENT_SCHEMA :
                    this.currentSchema = tag.valueString();
                    break;
                default :
                    /* ignore all others, we don't need them pre-parsed */
            }
        }
    }

    /**
     * Fill in the given oneRowChange with info from this LCR
     * 
     * @param oneRowChange where to set all the data
     */
    public void parseDataTypes(OneRowChange oneRowChange)
            throws UnsupportedEncodingException, ReplicatorException,
            SerialException, SQLException
    {
        oneColVal currentCol = null;
        columnValues = new ArrayList<oneColVal>();
        for (Iterator<PlogLCRTag> iterator = rawTags.iterator(); iterator
                .hasNext();)
        {
            PlogLCRTag tag = iterator.next();
            switch (tag.id)
            {
                case PlogLCRTag.TAG_COL_ID :
                    currentCol = new oneColVal();
                    columnValues.add(currentCol);
                    currentCol.id = tag.valueInt();
                    currentCol.columnSpec = oneRowChange.new ColumnSpec();
                    currentCol.columnSpec.setBlob(false);
                    currentCol.columnSpec.setNotNull(false);
                    currentCol.columnVal = oneRowChange.new ColumnVal();
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_COL_NAME :
                    currentCol.name = tag.valueString();
                    currentCol.columnSpec.setName(currentCol.name);
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_COL_TYPE :
                    currentCol.datatype = tag.valueString();
                    currentCol.columnSpec
                            .setTypeDescription(currentCol.datatype);
                    int tagTypeAsSQLType = currentCol.typeAsSQLType();
                    currentCol.columnSpec.setType(tagTypeAsSQLType);
                    /*
                     * NUMERIC is signed, all other are not numbers at all
                     */
                    currentCol.columnSpec.setSigned(
                            tagTypeAsSQLType == java.sql.Types.NUMERIC);
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_LOB_POSITION :
                    currentCol.lobPosition = tag.valueInt();
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_LOBOFFSET :
                    currentCol.lobOffset = tag.valueLong();
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_LOBLEN :
                    currentCol.lobLength = tag.valueLong();
                    iterator.remove();
                    break;

                case PlogLCRTag.TAG_PREIMAGE :
                case PlogLCRTag.TAG_POSTIMAGE :
                case PlogLCRTag.TAG_KEYIMAGE :
                case PlogLCRTag.TAG_LOBDATA :
                    currentCol.imageType = tag.id;
                    currentCol.rawVal = tag.rawData;
                    currentCol.parseValue(); /*
                                              * set columnVal.data or
                                              * columnVal.setValueNull and
                                              * columnSpec.length
                                              */
                    iterator.remove();
                    break;
                default :
                    /* ignore all others, we don't need them pre-parsed */
            }
        }
    }

    /**
     * Parse information about include plog = LOAD
     * 
     * @param parent parent PlatformLogger
     * @return new IncudePlog object
     */
    public PlogReaderThread.IncludePlog parseIncludePlogLCR(
            PlogReaderThread parent)
    {
        String filename = null;
        for (PlogLCRTag tag : rawTags)
        {
            switch (tag.id)
            {
                case PlogLCRTag.TAG_PLOG_FILENAME :
                    filename = tag.valueString();
                    break;
                case PlogLCRTag.TAG_PLOGSEQ :
                case PlogLCRTag.TAG_RBA :
                case PlogLCRTag.TAG_APPLY_NAME :
                case PlogLCRTag.TAG_MINE_UUID :
                case PlogLCRTag.TAG_SCN :
                case PlogLCRTag.TAG_PLOGNAME :
                    // these are present, but of no interest for us
                default :
                    /*
                     * ignore all others - not present in current plog version
                     */
            }
        }
        return parent.new IncludePlog(filename);
    }

    /**
     * Get plog sequence of current plog
     * 
     * @return plog sequence
     */
    public long getPlogId()
    {
        /* or should we keep it as separate filed? */
        return this.LCRid / 1000000000L;
    }

    /**
     * Get action type as Tungsten ActionType constant (convert our id into
     * Tungsten one)
     * 
     * @return plog sequence
     */
    public ActionType subtypeAsActionType() throws ReplicatorException
    {
        if (type != ETYPE_LCR_DATA)
        {
            throw new ReplicatorException(
                    "subtypeAsActionType should be called on DML (LCR_DATA) only");
        }
        switch (this.subtype)
        {
            case ESTYPE_LCR_INSERT :
                return ActionType.INSERT;
            case ESTYPE_LCR_UPDATE :
                return ActionType.UPDATE;
            case ESTYPE_LCR_DELETE :
                return ActionType.DELETE;
            case ESTYPE_LCR_LOB_WRITE :
                return ActionType.UPDATE;
            default :
                throw new ReplicatorException(
                        "subtypeAsActionType: unsupported data LCR subtype "
                                + subtype);
        }
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Convert byte[] array to hex string. Used for human-readable logging.
     * 
     * @param bytes input byte[] array
     * @return text string
     */
    public static String bytesToHex(byte[] bytes)
    {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++)
        {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Print summary of LCR contents.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName()).append(":");
        sb.append(" XID=").append(XID);
        sb.append(" type=").append(type);
        sb.append(" subtype=").append(subtype);
        sb.append(" SCN=").append(SCN);
        sb.append(" owner=").append(tableOwner);
        sb.append(" table=").append(tableName);
        return sb.toString();
    }
}
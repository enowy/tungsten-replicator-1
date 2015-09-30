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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.filter;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements a filter to eliminate Tungsten catalog data that should not reach
 * downstream replicators. The following table shows the rules for deciding what
 * to suppress or allow.
 * <table>
 * <tr>
 * <th>Table Name</th>
 * <th>DDL Operations?</th>
 * <th>DML Operations?</th>
 * </tr>
 * <tr>
 * <td>consistency</td>
 * <td>Suppress</td>
 * <td>Allow</td>
 * </tr>
 * <tr>
 * <td>heartbeat</td>
 * <td>Suppress</td>
 * <td>Allow UPDATE only</td>
 * </tr>
 * <tr>
 * <td>trep_commit_seqno</td>
 * <td>Suppress</td>
 * <td>Suppress</td>
 * </tr>
 * <tr>
 * <td>trep_shard</td>
 * <td>Suppress</td>
 * <td>Suppress</td>
 * </tr>
 * <tr>
 * <td>trep_shard_channel</td>
 * <td>Suppress</td>
 * <td>Suppress</td>
 * </tr>
 * </table>
 * For instance, we allow heartbeat table to pass through but suppress DDL. For
 * trep_commit_seqno we suppress all data.
 */
public class CatalogDataFilter implements Filter
{
    private static Logger logger = Logger.getLogger(CatalogDataFilter.class);

    private String                    tungstenSchema;
    private final SqlOperationMatcher parser = new MySQLOperationMatcher();

    /**
     * Sets the Tungsten schema explicitly. This is used for filter testing,
     * which runs without a pipeline.
     */
    public void setTungstenSchema(String tungstenSchema)
    {
        this.tungstenSchema = tungstenSchema;
    }

    /**
     * Filters catalog data using rules from the class header.
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();

        if (data == null)
            return event;

        for (Iterator<DBMSData> iterator = data.iterator(); iterator.hasNext();)
        {
            DBMSData dataElem = iterator.next();
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (Iterator<OneRowChange> iterator2 = rdata.getRowChanges()
                        .iterator(); iterator2.hasNext();)
                {
                    OneRowChange orc = iterator2.next();
                    if (filterRowChange(orc.getSchemaName(), orc.getTableName(),
                            orc.getAction()))
                    {
                        iterator2.remove();
                    }
                }
                if (rdata.getRowChanges().isEmpty())
                {
                    iterator.remove();
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                String schema = null;
                String table = null;
                int operation = -1;

                // Make a best effort to get parsing metadata on the statement.
                // Invoke parsing again if necessary.
                Object parsingMetadata = sdata.getParsingMetadata();
                if (parsingMetadata == null)
                {
                    String query = sdata.getQuery();
                    parsingMetadata = parser.match(query);
                    sdata.setParsingMetadata(parsingMetadata);
                }

                // Usually we have parsing metadata at this point.
                if (parsingMetadata != null
                        && parsingMetadata instanceof SqlOperation)
                {
                    SqlOperation parsed = (SqlOperation) parsingMetadata;
                    schema = parsed.getSchema();
                    table = parsed.getName();
                    operation = parsed.getOperation();
                    if (logger.isDebugEnabled())
                        logger.debug("Parsing found schema = " + schema
                                + " / table = " + table);
                }

                // If we don't have a schema, use the session default.
                if (schema == null)
                    schema = sdata.getDefaultSchema();

                // If the schema is still null this is not a catalog operation.
                if (schema == null)
                {
                    if (logger.isDebugEnabled())
                    {
                        final String query = sdata.getQuery();
                        logger.warn(
                                "Ignoring query : No schema found for this query from event "
                                        + event.getSeqno()
                                        + logQuery(query));
                    }
                    continue;
                }

                if (table == null)
                {
                    if (logger.isDebugEnabled())
                    {
                        final String query = sdata.getQuery();
                        logger.warn(
                                "Ignoring query : No table found for this query from event "
                                        + event.getSeqno() + logQuery(query));
                    }
                    continue;
                }
                
                // See if we want to filter this statement.
                if (filterStatement(schema, table, operation))
                {
                    iterator.remove();
                }
            }
        }

        // Don't drop events when dealing with fragmented events. (This could
        // drop the commit part)
        if (event.getFragno() == 0 && event.getLastFrag() && data.isEmpty())
        {
            return null;
        }
        return event;
    }

    private String logQuery(final String query)
    {
        return query != null
                ? " (" + query.substring(0, Math.min(query.length(), 200))
                        + "...)"
                : "";
    }

    // Returns true if the statement should be filtered because it belongs to
    // a Tungsten catalog operation (DDL or DML) that should not replicate.
    private boolean filterStatement(String schema, String table, int opcode)
    {
        // Lower-case string values.
        schema = schema.toLowerCase();
        table = table.toLowerCase();

        // Test for the catalog schema.
        if (schema.equals(tungstenSchema))
        {
            // If we match the catalog schema, now check each table.
            if ("trep_commit_seqno".equals(table))
                return true;
            else if ("trep_shard".equals(table))
                return true;
            else if ("trep_shard_channel".equals(table))
                return true;
            else if ("consistency".equals(table))
                return !isDML(opcode);
            else if ("heartbeat".equals(table))
                return (opcode != SqlOperation.UPDATE);
            else
                return false;
        }
        else
            return false;
    }

    // Returns true if the SQLOperation opcode corresponds to a DML operation.
    private boolean isDML(int opcode)
    {
        if (opcode == SqlOperation.DELETE)
            return true;
        else if (opcode == SqlOperation.INSERT)
            return true;
        else if (opcode == SqlOperation.REPLACE)
            return true;
        else if (opcode == SqlOperation.UPDATE)
            return true;
        else
            return false;
    }

    // Returns true if the row change should be filtered because it belongs to
    // a Tungsten catalog table that should not replicate.
    private boolean filterRowChange(String schema, String table,
            RowChangeData.ActionType action)
    {
        // Lower-case string values.
        schema = schema.toLowerCase();
        table = table.toLowerCase();

        // Test for the catalog schema.
        if (schema.equals(tungstenSchema))
        {
            // If we match the catalog schema, now check each table.
            if ("trep_commit_seqno".equals(table))
                return true;
            else if ("trep_shard".equals(table))
                return true;
            else if ("trep_shard_channel".equals(table))
                return true;
            else if ("heartbeat".equals(table))
                return action != RowChangeData.ActionType.UPDATE;
            else
                return false;
        }
        else
            return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Load the schema if not already specified explicitly.
        if (context != null && tungstenSchema == null)
        {
            tungstenSchema = context.getReplicatorSchemaName();
        }

        // Validate and force catalog schema name to lower case.
        if (tungstenSchema == null || tungstenSchema.length() == 0)
        {
            throw new ReplicatorException(
                    "No catalog schema name specified in configuration; unable to filter catalog operations correctly");
        }
        tungstenSchema = tungstenSchema.toLowerCase();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Nothing to be done.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Nothing to be done.
    }
}

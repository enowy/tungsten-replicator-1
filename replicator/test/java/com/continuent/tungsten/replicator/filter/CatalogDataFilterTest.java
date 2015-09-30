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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.EventGenerationHelper;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

import junit.framework.TestCase;

/**
 * This class implements a test of the TungstenCatalogFilter class. It confirms
 * that we skip over non-catalog data and follow the rules for
 * suppressing/allowing DDL and DML for catalog tables.
 */
public class CatalogDataFilterTest extends TestCase
{
    private FilterVerificationHelper filterHelper = new FilterVerificationHelper();
    private EventGenerationHelper    eventHelper  = new EventGenerationHelper();

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Verify that the catalog filter allows non-catalog statements and row
     * changes through. This includes cases where tables and rows have the same
     * schema.
     */
    public void testAcceptNonCatalogOps() throws Exception
    {
        CatalogDataFilter tcf = new CatalogDataFilter();
        tcf.setTungstenSchema("tungsten_foo");
        filterHelper.setFilter(tcf);

        // Verify accepted non-catalog statements. Schema can be default or
        // explicit.
        verifyStmtAccept(filterHelper, 0, null,
                "create table tungsten_bar.bar (id int primary key, data varchar(25))");
        verifyStmtAccept(filterHelper, 0, null,
                "create table tungsten_bar.trep_commit_seqno (id int primary key, data varchar(25))");
        verifyStmtAccept(filterHelper, 0, "my_default_schema",
                "insert into heartbeat(val) values(1)");
        verifyStmtAccept(filterHelper, 0, null,
                "insert into bar(val) values(1)");

        // Verify accepted non-catalog row changes.
        String names[] = {"id"};
        Long values[] = {new Long(99)};
        verifyRowInsertAccept(filterHelper, 0, "tungsten_bar",
                "trep_commit_seqno", names, values);
        verifyRowInsertAccept(filterHelper, 1, "tungsten_foo",
                "non_catalog_table", names, values);
        verifyRowInsertAccept(filterHelper, 2, "foo", "test2", names, values);

        filterHelper.done();
    }

    /**
     * Verify that we reject all DDL statements that involve catalog tables and
     * names including variations in upper/lower case.
     */
    public void testRejectCatalogTableDDL()
            throws ReplicatorException, InterruptedException
    {
        CatalogDataFilter tcf = new CatalogDataFilter();
        tcf.setTungstenSchema("tungsten_foo");
        filterHelper.setFilter(tcf);

        // Test all variations of catalog DDL.
        verifyStmtIgnore(filterHelper, 0, null,
                "CREATE TABLE tungsten_foo.consistency (db CHAR(64) NOT NULL, tbl CHAR(64) NOT NULL, id NUMBER NOT NULL, "
                        + "row_offset NUMBER NOT NULL, row_limit NUMBER NOT NULL, this_crc CHAR(40), this_cnt NUMBER, "
                        + "master_crc CHAR(40), master_cnt NUMBER, ts TIMESTAMP, method CHAR(32), PRIMARY KEY (db, tbl, id))");
        verifyStmtIgnore(filterHelper, 0, null,
                "create table tungsten_foo.heartbeat (id int primary key, data varchar(25))");
        verifyStmtIgnore(filterHelper, 2, "tungsten_foo",
                "ALTER TABLE TREP_COMMIT_SEQNO ADD my_new_col varchar2(45);");
        verifyStmtIgnore(filterHelper, 3, null,
                "DROP TABLE tungsten_foo.trep_shard");
        verifyStmtIgnore(filterHelper, 1, "tungsten_foo",
                "drop table tungsten_foo.trep_SHARD_channel cascade");

        filterHelper.done();
    }

    /**
     * Verify that we accept DML for the heartbeat and consistency tables but
     * reject it for other catalog tables.
     */
    public void testAcceptSomeCatalogDML() throws Exception
    {
        CatalogDataFilter tcf = new CatalogDataFilter();
        tcf.setTungstenSchema("tungsten_foo");
        filterHelper.setFilter(tcf);

        // Check statements.
        verifyStmtAccept(filterHelper, 0, null,
                "insert into tungsten_foo.consistency(val) values(1)");
        verifyStmtAccept(filterHelper, 0, null,
                "update tungsten_foo.heartbeat set seqno=100 where id=1");
        verifyStmtIgnore(filterHelper, 0, "tungsten_foo",
                "insert into heartbeat(val) values(1)");
        verifyStmtIgnore(filterHelper, 0, null,
                "update tungsten_foo.trep_commit_seqno sets seqno=100 where task_id=1");
        verifyStmtIgnore(filterHelper, 0, "tungsten_foo",
                "insert into tungsten_foo.trep_shard values(1, 2)");
        verifyStmtIgnore(filterHelper, 0, null,
                "DELETE FROM tungsten_foo.trep_shard_CHANNEL");

        // Verify row changes.
        String names[] = {"id"};
        Long values[] = {new Long(99)};
        verifyRowInsertAccept(filterHelper, 0, "tungsten_foo", "consistency",
                names, values);
        verifyRowUpdateAccept(filterHelper, 1, "tungsten_foo", "heartbeat",
                names, values, values);
        verifyRowInsertIgnore(filterHelper, 1, "tungsten_foo", "heartbeat",
                names, values);
        verifyRowInsertIgnore(filterHelper, 2, "tungsten_foo",
                "trep_commit_seqno", names, values);
        verifyRowInsertIgnore(filterHelper, 3, "tungsten_foo", "trep_shard",
                names, values);
        verifyRowInsertIgnore(filterHelper, 4, "tungsten_foo",
                "trep_shard_channel", names, values);

        filterHelper.done();
    }

    // Confirms that a particular event is accepted.
    private void verifyAccept(FilterVerificationHelper filterHelper,
            ReplDBMSEvent e) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e2 = filterHelper.filter(e);
        assertTrue("Should allow event, seqno=" + e.getSeqno(), e == e2);
    }

    // Confirms that a particular event is ignored.
    private void verifyIgnore(FilterVerificationHelper filterHelper,
            ReplDBMSEvent e) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e2 = filterHelper.filter(e);
        assertNull("Should ignore event: seqno=" + e.getSeqno(), e2);
    }

    // Confirms that a particular statement-based event is accepted.
    private void verifyStmtAccept(FilterVerificationHelper filterHelper,
            long seqno, String defaultSchema, String query)
                    throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromStatement(seqno, defaultSchema,
                query);
        verifyAccept(filterHelper, e);
    }

    // Confirms that a particular statement-based event is ignored.
    private void verifyStmtIgnore(FilterVerificationHelper filterHelper,
            long seqno, String defaultSchema, String query)
                    throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromStatement(seqno, defaultSchema,
                query);
        verifyIgnore(filterHelper, e);
    }

    // Confirms that a particular row insert is accepted.
    private void verifyRowInsertAccept(FilterVerificationHelper filterHelper,
            long seqno, String schema, String table, String[] names,
            Object[] values) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(seqno, schema, table,
                names, values, 0, true);
        verifyAccept(filterHelper, e);
    }

    // Confirms that a particular row update is accepted.
    private void verifyRowUpdateAccept(FilterVerificationHelper filterHelper,
            long seqno, String schema, String table, String[] names,
            Object[] values, Object[] keys)
                    throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromRowUpdate(seqno, schema, table,
                names, values, keys, 0, true);
        verifyAccept(filterHelper, e);
    }

    // Confirms that a particular row insert event is accepted.
    private void verifyRowInsertIgnore(FilterVerificationHelper filterHelper,
            long seqno, String schema, String table, String[] names,
            Object[] values) throws ReplicatorException, InterruptedException
    {
        ReplDBMSEvent e = eventHelper.eventFromRowInsert(seqno, schema, table,
                names, values, 0, true);
        verifyIgnore(filterHelper, e);
    }
}
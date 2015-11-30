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

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests operations on Oracle event IDs.
 */
public class TestPlogEventId
{
    /**
     * Demonstrate that we can parse a DML event.
     */
    @Test
    public void testDML() throws Exception
    {
        PlogEventId pei = new PlogEventId(
                "53198523#000a.008.0000e7ea#LAST#53198522#2194");
        Assert.assertEquals(53198523, pei.getCommitSCN());
        Assert.assertEquals("000a.008.0000e7ea", pei.getXid());
        Assert.assertEquals("LAST", pei.getChangeSeq());
        Assert.assertEquals(53198522, pei.getStartSCN());
        Assert.assertEquals(2194, pei.getStartPlog());
    }

    /**
     * Demonstrate that we can parse a DML event.
     */
    @Test
    public void testDDL() throws Exception
    {
        PlogEventId pei = new PlogEventId(
                "54206393##DDL at 0000.033b1fb6#LAST#54206393#2219");
        Assert.assertEquals(54206393, pei.getCommitSCN());
        Assert.assertEquals("#DDL at 0000.033b1fb6", pei.getXid());
        Assert.assertEquals("LAST", pei.getChangeSeq());
        Assert.assertEquals(54206393, pei.getStartSCN());
        Assert.assertEquals(2219, pei.getStartPlog());
    }

    /**
     * Demonstrate that we can parse an SCN value.
     */
    @Test
    public void testRestartSCN() throws Exception
    {
        PlogEventId pei = new PlogEventId("531985223");
        Assert.assertEquals(531985223, pei.getCommitSCN());
        Assert.assertEquals("", pei.getXid());
        Assert.assertEquals("", pei.getChangeSeq());
        Assert.assertEquals(0, pei.getStartSCN());
        Assert.assertEquals(Integer.MAX_VALUE, pei.getStartPlog());
    }
}
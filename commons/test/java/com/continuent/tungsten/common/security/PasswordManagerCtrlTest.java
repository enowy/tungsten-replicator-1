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
 * Initial developer(s): Ludovic Launer
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;

import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;

/**
 * Unit test against PasswordManagerCtrl.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class PasswordManagerCtrlTest
{
//    private static Logger       logger            = Logger.getLogger(PasswordManagerCtrlTest.class);
  
    
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    
    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();
    
    @Rule
    public final SystemErrRule systemErrRule = new SystemErrRule().enableLog();


    /**
     * Confirm that we can display help
     * @throws Exception 
     */
    @Test
    public void testDisplayHelp()
    {
       
        
        String argv[]= {"--help"};
        try
        {
            exit.expectSystemExitWithStatus(0);
//            exit.checkAssertionAfterwards(new Assertion() {
//                public void checkAssertion() {
//                  assertEquals("exit ...", "");
//                }
//              });
            
            PasswordManagerCtrl.main(argv);
            
            int i;
            i=2;
        }
        catch (Exception e)
        {
        }
        
        String out = systemOutRule.getLog();
        int i;
        i=2;
        
    }
    
    @Test
    public void testCreateUser()
    {
        
    }


}

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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.extractor.oracle.redo;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements complete management of the redo reader process (AKA vmrr). This
 * encapsulates operations to start, stop, monitor, and reset the vmrr process
 * and associated state used to read Oracle REDO logs. These are presented as
 * simple API calls for clients.
 */
public class RedoReaderManager
{
    private static Logger logger = Logger.getLogger(RedoReaderManager.class);

    // Properties required to control the redo reader manager.
    private String vmrrControlScript;
    private String replicateApplyName;

    /** Instantiates a new manager. */
    public RedoReaderManager()
    {
    }

    public String getVmrrControlScript()
    {
        return vmrrControlScript;
    }

    public void setVmrrControlScript(String vmrrControlScript)
    {
        this.vmrrControlScript = vmrrControlScript;
    }

    public String getReplicateApplyName()
    {
        return replicateApplyName;
    }

    public void setReplicateApplyName(String replicateApplyName)
    {
        this.replicateApplyName = replicateApplyName;
    }

    /**
     * Validates the vmrr script. This must be called before making other
     * management API calls.
     */
    public synchronized void initialize() throws ReplicatorException
    {
        // Confirm that the vmrr control script exists and is executable.
        File controlScript = new File(vmrrControlScript);
        if (controlScript.isFile() && controlScript.canExecute())
        {
            logger.info("Validated vmrr control script: path="
                    + controlScript.getAbsolutePath());
        }
        else
        {
            throw new ReplicatorException(
                    "Vmrr control script not found or not executable: path="
                            + controlScript.getAbsolutePath());
        }
    }

    /**
     * Idempotent operation to see if the vmrr process is running and if not
     * start it, returning when the process starts. The call returns without
     * taking further action if the process is already running.
     * 
     * @throws ReplicatorException Thrown if the process cannot be started
     */
    public synchronized void start() throws ReplicatorException
    {
        String command = vmrrControlScript + " start";
        logger.info("Starting vmrr process: " + command);
        execAndReturnStdout(command, true);

        if (!isRunning())
        {
            throw new ReplicatorException("Unable to start vmrr process");
        }
    }

    /**
     * Idempotent operation to see if the vmrr process is running and if so
     * start it. The call returns without taking further action if the process
     * is not running.
     * 
     * @throws ReplicatorException Thrown if the process stop operation fails,
     *             for example due to a missing control script.
     */
    public synchronized void stop() throws ReplicatorException
    {
        String command = vmrrControlScript + " stop";
        logger.info("Stopping vmrr process: " + command);
        execAndReturnStdout(command, true);
    }

    /**
     * Checks the status of the vmrr process and returns true if it is running.
     */
    public synchronized boolean isRunning() throws ReplicatorException
    {
        String command = vmrrControlScript + " status";
        if (logger.isDebugEnabled())
        {
            logger.debug("Checking vmrr process status: " + command);
        }
        int result = this.execAndReturnExitValue(command);
        return (result == 0);
    }

    /**
     * Runs a health check on the vmrr process, throwing an exception if it
     * fails.
     */
    public synchronized void healthCheck() throws ReplicatorException
    {
        String command = vmrrControlScript + " command healthcheck";
        if (logger.isDebugEnabled())
        {
            logger.debug("Running vmrr process healthcheck: " + command);
        }
        this.execAndReturnStdout(command, false);
    }

    /**
     * Redoes configuration on the vmrr process. This removes all state and
     * reruns installation.
     * 
     * @param startScn Optional start SCN. If null the vmrr process will be
     *            configured to start at the currently configured SCN
     */
    public synchronized void reset(String startScn) throws ReplicatorException
    {
        String command;
        if (startScn == null)
            command = vmrrControlScript + " reset";
        else
            command = vmrrControlScript + " reset " + startScn;

        if (logger.isDebugEnabled())
        {
            logger.debug("Resetting vmrr process: " + command);
        }
        String stdout = execAndReturnStdout(command, true);
        logger.info(stdout);
    }

    /**
     * Ask mine to register our extractor, so it can then set last obsolete
     * plog. We call this on every start of extractor, as it is harmless to do
     * it repeatedly.
     * 
     * @throws IOException
     */
    public synchronized void registerExtractor() throws ReplicatorException
    {
        String command = vmrrControlScript
                + " command PROCESS DISCONNECTED_APPLY REGISTER "
                + this.replicateApplyName;
        logger.info("Registering with mine process: " + command);
        execAndReturnStdout(command, false);
    }

    /**
     * Signal to mine our last obsolete plog.
     * 
     * @param newObsolete last obsolete plog sequence to set
     */
    public synchronized void reportLastObsoletePlog(int newObsolete)
            throws ReplicatorException
    {
        String command = vmrrControlScript
                + " command PROCESS DISCONNECTED_APPLY SET_OBSOLETE "
                + this.replicateApplyName + " " + newObsolete;
        logger.info("Signaling last obsolete plog file: " + command);
        execAndReturnStdout(command, false);
    }

    /**
     * Execute an OS command and return the result of stdout.
     * 
     * @param command Command to run
     * @param directLogging If true write stdtout and stderr directly to the log
     * @return Returns output of the command in a string
     * @throws ReplicatorException Thrown if command execution fails
     */
    private String execAndReturnStdout(String command, boolean directLogging)
            throws ReplicatorException
    {
        String[] osArray = {"sh", "-c", command};
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(osArray);
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing OS command: " + command);
        }
        if (directLogging)
        {
            pe.setStdOut(logger);
            pe.setStdErr(logger);
        }
        pe.run();
        if (logger.isDebugEnabled())
        {
            logger.debug("OS command stdout: " + pe.getStdout());
            logger.debug("OS command stderr: " + pe.getStderr());
            logger.debug("OS command exit value: " + pe.getExitValue());
        }
        if (!pe.isSuccessful())
        {
            String msg;
            if (directLogging)
            {
                msg = "OS command failed: command=" + command + " rc="
                        + pe.getExitValue();
            }
            else
            {
                msg = "OS command failed: command=" + command + " rc="
                        + pe.getExitValue() + " stdout=" + pe.getStdout()
                        + " stderr=" + pe.getStderr();
            }
            logger.error(msg);
            throw new ReplicatorException(msg);
        }
        return pe.getStdout();
    }

    /**
     * Execute an OS command and return the status code.
     * 
     * @param command Command to run
     * @return Returns process exit value
     */
    private int execAndReturnExitValue(String command)
    {
        String[] osArray = {"sh", "-c", command};
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(osArray);
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing OS command: " + command);
        }
        pe.run();
        if (logger.isDebugEnabled())
        {
            logger.debug("OS command stdout: " + pe.getStdout());
            logger.debug("OS command stderr: " + pe.getStderr());
            logger.debug("OS command exit value: " + pe.getExitValue());
        }
        return pe.getExitValue();
    }
}
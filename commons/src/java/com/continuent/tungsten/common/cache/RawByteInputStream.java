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

package com.continuent.tungsten.common.cache;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implements an input stream on a raw byte vector. The input stream reads first
 * from available in-memory byte buffers, then from storage.
 */
public class RawByteInputStream extends InputStream
{
    // Allocator for the byte vector.
    private final RawByteAllocator alloc;

    // Overall index of next byte to read.
    private long nextByteIndex = 0;

    // Buffer position and offset.
    private int bufferIndex  = 0;
    private int bufferOffset = 0;

    // Input stream for reading from storage.
    private FileInputStream fileInput = null;
    private BufferedInputStream input = null;

    RawByteInputStream(RawByteAllocator alloc)
    {
        this.alloc = alloc;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.io.InputStream#read()
     */
    @Override
    public int read() throws IOException
    {
        // Decide whether we should be reading from byte buffers or storage.
        if (nextByteIndex < alloc.memoryLength)
        {
            // See if we have anything to read at all.
            if (bufferIndex >= alloc.buffers.size())
                return -1;

            // Adjust overall position forward so next read will be at the
            // correct position.
            nextByteIndex++;

            // See if the current memory offset is at the end of a buffer and
            // rotate to next buffer accordingly.
            while (bufferOffset >= alloc.buffers.get(bufferIndex).length)
            {
                bufferIndex++;
                bufferOffset = 0;
            }

            // Return the next byte. We AND with x'FF' to remove automatic
            // sign-extension.
            return 0xFF & alloc.buffers.get(bufferIndex)[bufferOffset++];
        }
        else
        {
            // See if there is storage allocated. If not we are at EOF.
            if (alloc.cacheFile == null)
                return -1;

            // Adjust overall position forward so next read will be at the
            // correct position.
            nextByteIndex++;

            // Ensure we have an input stream to read data.
            if (fileInput == null)
            {
                fileInput = new FileInputStream(alloc.cacheFile);
                input = new BufferedInputStream(fileInput);
            }

            return input.read();
        }
    }

    /**
     * Provide efficient skipping mechanism that depends on ability to determine
     * offset location quickly in memory buffer or by seeking to a file
     * position.
     * 
     * @throws IOException
     * @see java.io.InputStream#skip(long)
     */
    @Override
    public long skip(long n) throws IOException
    {
        // Test for trival case.
        if (n == 0)
        {
            return 0;
        }

        // Set up counters for seek operation.
        long target = nextByteIndex + n;
        long skipped;
        int currentLength = (int) (alloc.memoryLength + alloc.storageLength);

        // Test cases.
        if (target > currentLength)
        {
            // We are past the end of the vector. Next read should return EOF.
            // If there is storage allocated see to end to ensure this is the
            // case.
            skipped = currentLength - nextByteIndex;
            nextByteIndex = currentLength;
            if (fileInput != null)
                seek(alloc.storageLength);
        }
        else if (target < alloc.memoryLength)
        {
            // We are in the memory buffers at the head of the vector.
            skipped = target - nextByteIndex;
            long amountToSkip = skipped;

            // Mark off the distance to skip, updating buffer pointers
            // accordingly.
            while (amountToSkip > 0)
            {
                int currentBufferLength = alloc.buffers.get(bufferIndex).length;
                if (bufferOffset
                        + amountToSkip >= alloc.buffers.get(bufferIndex).length)
                {
                    // We need to skip to the next buffer, resetting the offset
                    // to zero.
                    amountToSkip -= (currentBufferLength - bufferOffset);
                    bufferIndex++;
                    bufferOffset = 0;
                }
                else
                {
                    // We need to skip forward within the current buffer.
                    bufferOffset = (int) amountToSkip;
                    amountToSkip = 0;
                    nextByteIndex = target;
                }
            }
        }
        else
        {
            // We are in the storage part of the vector. Seek to
            // target position after subtracting allocated memory.
            if (fileInput == null)
            {
                fileInput = new FileInputStream(alloc.cacheFile);
                input = new BufferedInputStream(fileInput);
            }
            seek(target - alloc.memoryLength);
            skipped = target - nextByteIndex;
            nextByteIndex = target;
        }

        // Return number of bytes actually skipped.
        return skipped;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException
    {
        // Close file input if we were using it.
        if (fileInput != null)
            fileInput.close();
        
        if(input != null)
            input.close();
    }

    // Seek to a particular position in storage.
    private void seek(long offset) throws IOException
    {
        fileInput.getChannel().position(offset);
    }
}
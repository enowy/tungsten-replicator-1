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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Implements a unit test of operations on RawByteCache, which manages
 * extensible byte vectors.
 */
public class RawByteCacheTest
{
    private static Logger logger = Logger.getLogger(RawByteCacheTest.class);

    /**
     * Verify that we can prepare and release a simple cache.
     */
    @Test
    public void testCreate() throws Exception
    {
        File testDir = this.prepareTestDir("testCreate");
        RawByteCache cache = new RawByteCache(testDir, 100, 10, 5);
        cache.prepare();
        logger.info("Cache: " + cache.toString());
        cache.release();
    }

    /**
     * Verify we can allocate, add content to, and deallocate a byte vector.
     */
    @Test
    public void testAllocVector() throws Exception
    {
        File testDir = this.prepareTestDir("testAllocVector");
        RawByteCache cache = new RawByteCache(testDir, 100, 10, 5);
        byte[] vector = makeVector(5);
        cache.prepare();

        // Add a vector and ensure that it is allocated.
        cache.allocate("test");
        cache.append("test", vector);
        Assert.assertEquals("cache size", 1, cache.getSize());
        logger.info("Cache: " + cache.toString());

        // Read the vector back.
        InputStream byteInput = cache.allocateStream("test");
        readAndVerifyVector(vector, byteInput);

        // Deallocate.
        cache.deallocate("test");
        Assert.assertEquals("cache size", 0, cache.getSize());
        logger.info("Cache: " + cache.toString());

        cache.release();
    }

    /**
     * Verify that if we write a vector using a single buffer *and* the vector
     * is within single vector byte limits it remains within memory. Otherwise
     * the vector bytes are written to storage.
     */
    @Test
    public void testSpillToStorage() throws Exception
    {
        File testDir = this.prepareTestDir("testSpillToStorage");
        RawByteCache cache = new RawByteCache(testDir, 100, 10, 5);
        cache.prepare();

        // Test various
        allocateAndTestVector(cache, "v1", 5, 5, 0);
        allocateAndTestVector(cache, "v2", 10, 10, 0);
        allocateAndTestVector(cache, "v3", 11, 0, 11);
        allocateAndTestVector(cache, "v4", 100, 0, 100);

        cache.release();
    }

    /**
     * Verify that if we write a vector using multiple buffers, any buffer that
     * exceeds the single object limit will go to storage.
     */
    @Test
    public void testSpillPartialVectorsToStorage() throws Exception
    {
        File testDir = this.prepareTestDir("testSpillPartialVectorsToStorage");
        RawByteCache cache = new RawByteCache(testDir, 100, 5, 5);
        cache.prepare();

        // Test various sizes of buffers that overflow.
        allocateAndTestVector(cache, "v1", 4, 4, 0, 2);
        allocateAndTestVector(cache, "v1", 6, 4, 2, 2);
        allocateAndTestVector(cache, "v2", 12, 0, 12, 6);

        cache.release();
    }

    /**
     * Verify that if the cache memory is exhausted all further objects will go
     * to storage.
     */
    @Test
    public void testCacheMemoryExhausted() throws Exception
    {
        File testDir = this.prepareTestDir("testCacheMemoryExhausted");
        RawByteCache cache = new RawByteCache(testDir, 100, 100, 5);
        cache.prepare();

        // Put a single 100 byte vector in the cache to fill it.
        byte[] v1 = makeVector(100);
        cache.allocate("v1");
        cache.append("v1", v1);

        // Show that any further vector size goes to storage.
        allocateAndTestVector(cache, "v2", 1, 100, 1);
        allocateAndTestVector(cache, "v3", 100, 100, 100);
        allocateAndTestVector(cache, "v4", 10000, 100, 10000);

        // Show that removing the first vector means that new vectors can go
        // into memory.
        cache.deallocate("v1");
        allocateAndTestVector(cache, "v5", 1, 1, 0);

        cache.release();
    }

    /**
     * Verify that we can write and read a very large vector accurately.
     */
    @Test
    public void testLargeVector() throws Exception
    {
        File testDir = this.prepareTestDir("testLargeVector");
        RawByteCache cache = new RawByteCache(testDir, 500000, 100000, 5);
        cache.prepare();

        // Test a vector of 1M bytes writing 10K bytes at a time.
        allocateAndTestVector(cache, "v1", 1000000, 100000, 900000, 10000);

        cache.release();
    }

    /**
     * Verify we can resize a vector regardless of whether said vector is in
     * storage or in memory.
     */
    @Test
    public void testResize() throws Exception
    {
        File testDir = this.prepareTestDir("testResize");
        RawByteCache cache = new RawByteCache(testDir, 10, 10, 5);
        cache.prepare();

        // Fully in memory: Resize a vector from 5 bytes to 2 bytes.
        resizeAndReadVector(cache, "v1", 5, 5, 2);

        // Memory + storage: Resize a vector from 20 bytes to 11, 10, 9.
        resizeAndReadVector(cache, "v2", 20, 5, 11);
        resizeAndReadVector(cache, "v3", 20, 5, 10);
        resizeAndReadVector(cache, "v4", 20, 5, 9);

        // Full in storage.
        resizeAndReadVector(cache, "v2", 20, 20, 11);
        resizeAndReadVector(cache, "v3", 20, 20, 10);
        resizeAndReadVector(cache, "v4", 20, 20, 9);
    }

    /**
     * Verify that the cache supports a large number of objects in storage even
     * when there are limited file descriptors and cleans up all files
     * afterwards.
     */
    @Test
    public void testNumerousFiles() throws Exception
    {
        File testDir = this.prepareTestDir("testCacheCleanup");
        RawByteCache cache = new RawByteCache(testDir, 100, 10000, 5);
        cache.prepare();

        // Put 2000 files in the cache by allocating vectors larger than 100
        // bytes.
        byte[] v = makeVector(200);
        for (int i = 0; i < 2000; i++)
        {
            String key = "v" + i;
            cache.allocate(key);
            cache.append(key, v);
            if (i > 0 && (i + 1) % 100 == 0)
                logger.info("Allocated vectors: " + i);
        }
        logger.info("Cache: " + cache.toString());

        // Deallocate and ensure no files remain.
        cache.release();
        File[] cachedFiles = testDir.listFiles();
        if (cachedFiles.length > 0)
        {
            for (File f : cachedFiles)
            {
                logger.error("Unexpected cache file after release: "
                        + f.getAbsolutePath());
            }
            throw new Exception("Cache contains files after release");
        }
    }

    /**
     * Create test directory, removing any previous directory.
     */
    private File prepareTestDir(String name) throws Exception
    {
        File testDir = new File(name);
        if (testDir.exists())
        {
            for (File child : testDir.listFiles())
            {
                child.delete();
            }
            testDir.delete();
        }

        if (testDir.exists())
            throw new Exception(
                    "Unable to clear test dir: " + testDir.getAbsolutePath());

        testDir.mkdirs();
        return testDir;
    }

    /** Creates a test vector. */
    private byte[] makeVector(int size)
    {
        byte[] vector = new byte[size];
        for (int i = 0; i < size; i++)
        {
            vector[i] = (byte) (i % 256);
        }
        return vector;
    }

    /** Reads back from stream and compares a byte vector. */
    private void readAndVerifyVector(byte[] oracle, InputStream testInput)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int c;
        while ((c = testInput.read()) != -1)
        {
            baos.write(c);
        }
        baos.flush();
        byte[] testBytes = baos.toByteArray();
        Assert.assertArrayEquals("Checking bit vector contents", oracle,
                testBytes);
    }

    /**
     * Creates, loads, and validates memory/storage for vector of given size,
     * appending all bytes at once.
     */
    private void allocateAndTestVector(RawByteCache cache, String key, int size,
            int expectedMemoryBytes, int expectedStorageBytes)
                    throws IOException
    {
        this.allocateAndTestVector(cache, key, size, expectedMemoryBytes,
                expectedStorageBytes, size);
    }

    /**
     * Creates, loads, and validates memory/storage for vector of given size.
     */
    private void allocateAndTestVector(RawByteCache cache, String key, int size,
            int expectedMemoryBytes, int expectedStorageBytes, int chunkSize)
                    throws IOException
    {
        // Add a vector.
        logger.info("Testing vector: key=" + key + " size=" + size);
        byte[] vector = makeVector(size);
        cache.allocate(key);
        int offset = 0;
        while (offset < size)
        {
            cache.append(key, getChunk(vector, offset, chunkSize));
            offset += chunkSize;
        }
        logger.info("Cache: " + cache.toString());

        // Test expected allocated memory and storage.
        Assert.assertEquals("Vector memory bytes, size=" + size,
                expectedMemoryBytes, cache.getCurrentMemoryBytes());
        Assert.assertEquals("Vector storage bytes, size=" + size,
                expectedStorageBytes, cache.getCurrentStorageBytes());

        // Read the vector back.
        InputStream byteInput = cache.allocateStream(key);
        readAndVerifyVector(vector, byteInput);

        // Deallocate.
        cache.deallocate(key);
    }

    /**
     * Creates, loads, resizes, and validates memory/storage for vector of given
     * size.
     */
    private void resizeAndReadVector(RawByteCache cache, String key, int size,
            int chunkSize, int newSize) throws IOException
    {
        // Add a vector.
        logger.info("Testing vector: key=" + key + " size=" + size);
        byte[] vector = makeVector(size);
        cache.allocate(key);
        int offset = 0;
        while (offset < size)
        {
            cache.append(key, getChunk(vector, offset, chunkSize));
            offset += chunkSize;
        }
        logger.info("Cache: " + cache.toString());
        Assert.assertEquals("Cache bytes, size=" + size, size,
                cache.getCurrentMemoryBytes() + cache.getCurrentStorageBytes());

        // Resize the vector.
        cache.resize(key, newSize);
        logger.info("Cache: " + cache.toString());
        Assert.assertEquals("Cache bytes, size=" + newSize, newSize,
                cache.getCurrentMemoryBytes() + cache.getCurrentStorageBytes());

        // Read the vector back.
        InputStream byteInput = cache.allocateStream(key);
        readAndVerifyVector(getChunk(vector, 0, newSize), byteInput);

        // Deallocate.
        cache.deallocate(key);
    }

    /**
     * Returns a chunk of a vector.
     */
    private byte[] getChunk(byte[] vector, int offset, int chunkSize)
    {
        byte[] chunk = new byte[chunkSize];
        for (int i = 0; i < chunkSize; i++)
        {
            chunk[i] = vector[offset + i];
        }
        return chunk;
    }
}
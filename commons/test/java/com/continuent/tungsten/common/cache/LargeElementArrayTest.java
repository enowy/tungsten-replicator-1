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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Implements a unit test of operations on RawByteCache, which manages
 * extensible byte vectors.
 */
public class LargeElementArrayTest
{
    private static Logger logger = Logger
            .getLogger(LargeElementArrayTest.class);

    /**
     * Verify that we can create a large object array when caching is enabled,
     * add an element, and read it back.
     */
    @Test
    public void testArraySimpleLocal() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArraySimpleCache");
        RawByteCache cache = new RawByteCache(testDir, 10000, 1000, 5);
        cache.prepare();
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, 10);
        Assert.assertEquals("Empty array size", 0, loa.size());

        // Add and read back an object.
        SampleObject object1 = new SampleObject(1, "string", -0.5);
        logger.info("Object1: " + object1.toString());
        loa.add(object1);
        Assert.assertEquals("Single element array size", 1, loa.size());
        SampleObject object2 = loa.get(0);
        logger.info("Object2: " + object2.toString());
        Assert.assertEquals("Original vs. storage object", object1, object2);

        // Release the cache.
        cache.release();
    }

    /**
     * Verify that we can create a large object array when caching is enabled,
     * add an element, and read it back.
     */
    @Test
    public void testArraySimpleCache() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArraySimpleCache");
        RawByteCache cache = new RawByteCache(testDir, 10000, 1000, 5);
        cache.prepare();
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, 0);
        Assert.assertEquals("Empty array size", 0, loa.size());

        // Add and read back an object.
        SampleObject object1 = new SampleObject(1, "string", -0.5);
        logger.info("Object1: " + object1.toString());
        loa.add(object1);
        Assert.assertEquals("Single element array size", 1, loa.size());
        SampleObject object2 = loa.get(0);
        logger.info("Object2: " + object2.toString());
        Assert.assertEquals("Original vs. storage object", object1, object2);

        // Release the cache.
        cache.release();
    }

    /**
     * Verify that we can create and read back arrays of various sizes.
     */
    @Test
    public void testArrayBufferAndCache() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArrayMulti");
        RawByteCache cache = new RawByteCache(testDir, 10000, 1000, 5);
        cache.prepare();

        for (int size = 1; size < 40; size++)
        {
            createAndCompareList(cache, 20, size);
        }

        // Release the cache.
        cache.release();
    }

    /** Creates a list and compares values. */
    public void createAndCompareList(RawByteCache cache, int localBufferSize,
            int objectCount) throws IOException
    {
        // Create an array and add objects.
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, localBufferSize);
        List<SampleObject> objects = this.createObjectList(loa, objectCount);

        LargeObjectScanner<SampleObject> scanner = loa.scanner();
        int count = 0;
        while (scanner.hasNext())
        {
            SampleObject original = objects.get(count);
            SampleObject stored = scanner.next();
            Assert.assertEquals(
                    "Original vs. stored object: object=" + original, original,
                    stored);
            if (count > 0 && (count + 1) % 50 == 0)
                logger.info("Checked objects: " + count);

            count++;
        }

        Assert.assertEquals("Checking scanned object count", objects.size(),
                count);
        loa.release();
    }

    /**
     * Verify that we can create a very large array of objects and read them
     * back using the index.
     */
    @Test
    public void testArrayMulti() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArrayMulti");
        RawByteCache cache = new RawByteCache(testDir, 10000, 1000, 5);
        cache.prepare();
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, 100);

        // Insert 1000 objects.
        List<SampleObject> objects = this.createObjectList(loa, 1000);

        // Compare list of originals to the array.
        for (int i = 0; i < 1000; i++)
        {
            SampleObject original = objects.get(i);
            SampleObject stored = loa.get(i);
            Assert.assertEquals(
                    "Original vs. stored object: object=" + original, original,
                    stored);
            if (i > 0 && (i + 1) % 50 == 0)
                logger.info("Checked objects: " + i);
        }

        // Release the cache.
        cache.release();
    }

    /**
     * Verify that we can create a large array then fetch members back using a
     * scanner.
     */
    @Test
    public void testArrayIteration() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArrayIteration");
        RawByteCache cache = new RawByteCache(testDir, 25000000, 12500000, 5);
        cache.prepare();
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, 0);

        // Insert 100K objects.
        List<SampleObject> objects = this.createObjectList(loa, 100000);
        logger.info("Cache: " + cache.toString());

        // Compare list of originals to the array.
        LargeObjectScanner<SampleObject> scanner = loa.scanner();
        for (int i = 0; i < 100000; i++)
        {
            SampleObject original = objects.get(i);
            Assert.assertTrue("Scanner has available object: iteration=" + i,
                    scanner.hasNext());
            SampleObject stored = scanner.next();
            Assert.assertEquals(
                    "Original vs. stored object: object=" + original, original,
                    stored);
            if (i > 0 && (i + 1) % 10000 == 0)
                logger.info("Checked objects: " + i);
        }

        // Prove that we are at the end of the list.
        Assert.assertFalse("At end of the large array", scanner.hasNext());

        // Release the cache.
        cache.release();
    }

    /**
     * Verify that we can resize arrays of various sizes. In each case we cut
     * the array in half covering cases where:
     * <ol>
     * <li>Array starts and ends in memory</li>
     * <li>Array starts in storage and ends in memory</li>
     * <li>Array starts and ends in storage</li>
     * </ol>
     * The serialized sample object is 130 bytes.
     */
    @Test
    public void testArrayResize() throws Exception
    {
        // Create the array.
        File testDir = this.prepareTestDir("testArrayResize");
        RawByteCache cache = new RawByteCache(testDir, 100000, 1000, 5);
        cache.prepare();

        // Verify different resizing cases where the array is still buffered as
        // Java objects.
        allocateAndResizeList(cache, 5, 4, 0);
        allocateAndResizeList(cache, 5, 4, 2);
        allocateAndResizeList(cache, 5, 4, 4);

        // Verify different resizing cases where the array has spilled to cache.
        allocateAndResizeList(cache, 2, 4, 0);
        allocateAndResizeList(cache, 2, 4, 2);
        allocateAndResizeList(cache, 2, 4, 4);
        allocateAndResizeList(cache, 5, 10, 0);
        allocateAndResizeList(cache, 5, 10, 5);
        allocateAndResizeList(cache, 5, 10, 10);
        allocateAndResizeList(cache, 20, 100, 0);
        allocateAndResizeList(cache, 20, 100, 50);
        allocateAndResizeList(cache, 20, 100, 100);

        // Release the cache.
        cache.release();
    }

    /**
     * Append objects to a large object array, returning a list of said objects
     * in insert order.
     */
    private List<SampleObject> createObjectList(
            LargeObjectArray<SampleObject> loa, int size)
    {
        List<SampleObject> objects = new ArrayList<SampleObject>(size);

        // Add 1000 objects to array and save originals so we can check later.
        for (int i = 0; i < size; i++)
        {
            SampleObject o = new SampleObject(i, "string*value", -0.5);
            loa.add(o);
            objects.add(o);
        }
        Assert.assertEquals("Full array size", size, loa.size());
        return objects;
    }

    /**
     * Allocates a list and resizes it.
     */
    private void allocateAndResizeList(RawByteCache cache, int localBufferSize,
            int size1, int size2) throws IOException
    {
        // Generate object list.
        LargeObjectArray<SampleObject> loa = new LargeObjectArray<SampleObject>(
                cache, localBufferSize);
        List<SampleObject> originals = createObjectList(loa, size1);
        logger.info("Pre-resize list added: size=" + size1 + " cache: "
                + cache.toString());

        // Resize the list.
        loa.resize(size2);
        logger.info(
                "List resized: size=" + size1 + " cache: " + cache.toString());

        // Ensure the list is resized and that remaining object compare.
        Assert.assertEquals("Resized list size", size2, loa.size());
        LargeObjectScanner<SampleObject> scanner = loa.scanner();
        for (int i = 0; i < size2; i++)
        {
            SampleObject original = originals.get(i);
            Assert.assertTrue("Scanner has available object: iteration=" + i,
                    scanner.hasNext());
            SampleObject stored = scanner.next();
            Assert.assertEquals(
                    "Original vs. stored object: object=" + original, original,
                    stored);
        }

        // Prove that we are at the end of the list.
        Assert.assertFalse("At end of the large array", scanner.hasNext());

        // Release the list.
        loa.release();
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
}
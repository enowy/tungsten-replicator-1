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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Manages a cache of resizable raw byte vectors. This class is ensures that the
 * cache does not exceed resource limits, which include the following:
 * <ul>
 * <li>Number of bytes in memory for a single vector</li>
 * <li>Number of bytes in memory for all vectors</li>
 * <li>Number of open file descriptors writing to storage</li>
 * </ul>
 * Each vector has a key associated with it. It is the responsibility of clients
 * to create a unique key. The cache class offers methods to allocate, append,
 * read, resize, and deallocate vectors using that key.
 * <p/>
 * Individual vectors are initially as a set of extents in memory. Once the
 * vector exceeds the number of memory bytes allowed for a single vector, or the
 * cache exceeds remaining memory bytes, new bytes are written to storage. To
 * keep things sane, bytes are effectively immutable unless the vector is
 * resized to a smaller length, in which case any extra bytes are truncated.
 */
public class RawByteCache
{
    private static final Logger logger = Logger.getLogger(RawByteCache.class);

    // Cache settings. Initial maximum sizes will cause everything to remain in
    // memory.
    private final File cacheDir;
    private final long maxCacheBytes;
    private final long maxObjectBytes;
    private final int  maxOpenFiles;

    // Cache control properties.
    private long                              currentMemoryBytes    = 0;
    private long                              currentStorageBytes   = 0;
    private Map<Object, RawByteAllocator>     rawByteAllocatorCache = new HashMap<Object, RawByteAllocator>();
    private IndexedLRUCache<FileOutputStream> outputStreamCache;

    /**
     * Instantiates a new cache.
     * 
     * @param cacheDir Location of cache files
     * @param maxCacheBytes Maximum overall bytes to store in memory; above this
     *            level all new vectors flush to storage
     * @param maxObjectBytes Maximum bytes to store for a single vector; bytes
     *            go to storage above this level
     * @param maxOpenFiles Maximum number of cached files that may be open at
     *            once
     */
    public RawByteCache(File cacheDir, long maxCacheBytes, long maxObjectBytes,
            int maxOpenFiles)
    {
        this.cacheDir = cacheDir;
        this.maxCacheBytes = maxCacheBytes;
        this.maxObjectBytes = maxObjectBytes;
        this.maxOpenFiles = maxOpenFiles;
    }

    /** Return number of elements in the cache. */
    public long getSize()
    {
        return rawByteAllocatorCache.size();
    }

    /** Return number of bytes of memory used by cache. */
    public long getCurrentMemoryBytes()
    {
        return currentMemoryBytes;
    }

    /** Return number of bytes of storage used by cache. */
    public long getCurrentStorageBytes()
    {
        return currentStorageBytes;
    }

    /**
     * Prepares the cache for use.
     */
    public synchronized void prepare()
    {
        // Ensure object cache exists.
        logger.info("Opening byte vector cache: " + cacheDir.getAbsolutePath());
        cacheDir.mkdirs();
        if (!cacheDir.exists() || !cacheDir.isDirectory())
        {
            throw new RuntimeException(
                    "Unable to access byte vector cache directory: "
                            + cacheDir.getAbsolutePath());
        }

        // Delete any files currently in the cache.
        for (File cachedFile : cacheDir.listFiles())
        {
            if (!cachedFile.delete())
            {
                throw new RuntimeException("Unable to clear cached vector: "
                        + cachedFile.getAbsolutePath());
            }
        }

        // Create the LRU cache for managing output streams with an inner class
        // to release streams after use.
        CacheResourceManager<FileOutputStream> cacheResourceManager = new CacheResourceManager<FileOutputStream>()
        {
            public void release(FileOutputStream stream)
            {
                try
                {
                    stream.close();
                }
                catch (IOException e)
                {
                }
            }
        };
        outputStreamCache = new IndexedLRUCache<FileOutputStream>(maxOpenFiles,
                cacheResourceManager);
    }

    /**
     * Releases the cache and deletes files.
     */
    public synchronized void release()
    {
        // Invalidate the OutputStream cache, which closes any outstanding
        // streams.
        if (outputStreamCache != null)
            outputStreamCache.invalidateAll();

        // Clear the cache.
        if (cacheDir.exists() && cacheDir.isDirectory())
        {
            for (File cachedFile : cacheDir.listFiles())
            {
                if (!cachedFile.delete())
                {
                    logger.warn("Unable to clear cached vector: "
                            + cachedFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Allocates a cached vector, failing if the vector is already present.
     * 
     * @param key Object reference
     */
    public synchronized void allocate(String key)
    {
        if (this.rawByteAllocatorCache.get(key) == null)
        {
            RawByteAllocator alloc = new RawByteAllocator();
            alloc.key = key;
            rawByteAllocatorCache.put(key, alloc);
        }
        else
        {
            throw new RuntimeException(
                    "Attempt to allocate existing vector: key=" + key);
        }
    }

    /**
     * Deallocates the cached vector, freeing all associated resources. Fails if
     * the vector is not cached.
     * 
     * @param key Object reference
     */
    public void deallocate(String key)
    {
        RawByteAllocator alloc = rawByteAllocatorCache.remove(key);
        if (key == null)
        {
            logger.warn("Attempt to deallocate unknown vector from cache: key="
                    + key);
        }
        else
        {
            // Null out array to free memory buffers.
            alloc.buffers = null;
            currentMemoryBytes -= alloc.memoryLength;

            // Invalidate any open output stream on the file system and
            // delete storage.
            if (alloc.storageLength > 0)
            {
                outputStreamCache.invalidate(key);
                alloc.cacheFile.delete();
                this.currentStorageBytes -= alloc.storageLength;
            }
        }
    }

    /**
     * Adds a range of bytes to the end of the vector.
     * 
     * @param key Object reference
     * @param bytes Byte buffer to write
     */
    public synchronized void append(String key, byte[] bytes)
    {
        // Locate the vector and compute expected sizes of vector and overall
        // cache.
        RawByteAllocator alloc = findRawByteAllocator(key);
        long newObjectBytes = alloc.memoryLength + bytes.length;
        long newCacheBytes = this.currentMemoryBytes + bytes.length;

        // There are two cases to consider: either the new buffer fits in memory
        // or it does not.
        if (newObjectBytes <= maxObjectBytes && newCacheBytes <= maxCacheBytes)
        {
            // Case 1: New buffer fits within single vector and cache memory
            // limits.
            alloc.buffers.add(bytes);

            // Update cache sizes.
            alloc.memoryLength += bytes.length;
            currentMemoryBytes += bytes.length;
        }
        else
        {
            // Case 2: New buffer exceeds resource limits, hence must go to
            // storage.

            // We start by ensuring that the storage file exists
            if (alloc.storageLength == 0)
            {
                // Generate the storage file name.
                alloc.cacheFile = new File(cacheDir, key);
            }

            // Write buffer to storage.
            try
            {
                // Fetch the storage output stream from the LRU cache. If it
                // does not exist, create a new one.
                FileOutputStream output = outputStreamCache.get(key);
                if (output == null)
                {
                    // Create and store the output stream.
                    output = new FileOutputStream(alloc.cacheFile);
                    outputStreamCache.put(key, output);

                    // Seek to the end of the currently stored information so
                    // the next write picks up the previous position (including
                    // 0 if this is the first write).
                    FileChannel channel = output.getChannel();
                    channel.position(alloc.storageLength);
                }

                output.write(bytes);
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format(
                        "Unable to append bytes to storage: key=%s file=%s buffer length=%d",
                        key, alloc.cacheFile.getAbsolutePath(), bytes.length));
            }

            // Adjust object length and storage cache size.
            alloc.storageLength += bytes.length;
            currentStorageBytes += bytes.length;
        }

    }

    /**
     * Returns an input stream to read the vector starting from the first byte.
     * 
     * @param key Object reference
     * @return An InputStream to read the vector
     */
    public InputStream allocateStream(String key)
    {
        RawByteAllocator alloc = findRawByteAllocator(key);
        InputStream input = new RawByteInputStream(alloc);
        return input;
    }

    /**
     * Current size of the vector.
     * 
     * @param key Object reference
     * @return Size in bytes
     */
    public long size(String key)
    {
        RawByteAllocator alloc = findRawByteAllocator(key);
        return alloc.memoryLength + alloc.storageLength;
    }

    /**
     * Resizes the byte vector to a specified length.
     * 
     * @param key Object reference
     * @param length New length of vector
     */
    public void resize(String key, long length)
    {
        RawByteAllocator alloc = findRawByteAllocator(key);

        // First step of resizing is to drop storage.
        if (alloc.cacheFile != null)
        {
            long newFileLength = length - alloc.memoryLength;
            if (newFileLength > 0)
            {
                // This truncate operation is entirely in storage and just
                // shortens the file.
                int excessLength = (int) (alloc.storageLength - newFileLength);
                FileOutputStream output = outputStreamCache.get(key);
                try
                {
                    if (output == null)
                    {
                        // Create and store the output stream.
                        output = new FileOutputStream(alloc.cacheFile, true);
                        outputStreamCache.put(key, output);
                    }

                    // Since this is an existing file, we need to position
                    // at the end.
                    FileChannel channel = output.getChannel();
                    channel.truncate(newFileLength);
                    channel.position(newFileLength);

                    // Flush and then invalidate the LRU cache entry to close
                    // the file.
                    output.flush();
                    outputStreamCache.invalidate(key);

                    // Adjust length of object as well as cache storage
                    // bytes.
                    alloc.storageLength = newFileLength;
                    currentStorageBytes -= excessLength;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(String.format(
                            "Unable to truncate vector in storage: key=%s file=%s length=%d truncated length=%d",
                            key, alloc.cacheFile.getAbsolutePath(),
                            alloc.memoryLength + alloc.storageLength, length));
                }

                // Operation affects only storage, so we can return now.
                return;
            }
            else
            {
                // Storage file is deleted.
                outputStreamCache.invalidate(key);
                alloc.cacheFile.delete();
                alloc.cacheFile = null;
                currentStorageBytes -= alloc.storageLength;
                alloc.storageLength = 0;
            }
        }

        // Now we have to see if any buffers should be truncated.
        int lastBuffer = -1;
        int totalBufferBytes = 0;
        for (int i = 0; i < alloc.buffers.size(); i++)
        {
            byte[] buffer = alloc.buffers.get(i);
            totalBufferBytes += buffer.length;

            if (totalBufferBytes == length)
            {
                // We have to truncate any succeeding buffers.
                lastBuffer = i;
                break;
            }
            else if (totalBufferBytes > length)
            {
                // We have to truncate any succeeding buffers *and* part of
                // this one.
                lastBuffer = i;
                int excessLength = totalBufferBytes - (int) length;
                int shortenedBufferLength = buffer.length - excessLength;
                byte[] shortenedBuffer = Arrays.copyOf(buffer,
                        shortenedBufferLength);
                alloc.buffers.set(i, shortenedBuffer);

                // Subtracted dropped bytes from cache and vector size.
                currentMemoryBytes -= excessLength;
                alloc.memoryLength -= excessLength;
                break;
            }
        }

        // Remove excess buffers, if any. Subtract the size of each excess
        // buffer from the vector and cache length.
        if (lastBuffer > -1)
        {
            for (int i = alloc.buffers.size() - 1; i > lastBuffer; i--)
            {
                byte[] buffer = alloc.buffers.remove(i);
                currentMemoryBytes -= buffer.length;
                alloc.memoryLength -= buffer.length;
            }
        }
    }

    /**
     * Finds an existing RawByteAllocator or throws an exception.
     */
    private RawByteAllocator findRawByteAllocator(String key)
    {
        RawByteAllocator alloc = rawByteAllocatorCache.get(key);
        if (alloc == null)
        {
            throw new RuntimeException("Non-existence vector: key=" + key);
        }
        return alloc;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName()).append(":");
        sb.append(" elements=" + this.rawByteAllocatorCache.size());
        sb.append(" memoryBytes=" + this.currentMemoryBytes);
        sb.append(" storageBytes=" + this.currentStorageBytes);
        return sb.toString();
    }
}
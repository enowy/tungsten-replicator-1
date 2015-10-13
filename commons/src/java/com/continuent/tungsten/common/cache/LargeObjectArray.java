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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements an unbounded list that stores objects in the list in serialized
 * form using a RawByteCache that automatically flushes to storage to enable
 * unlimited list sizes.
 * </p>
 * Array elements are separately serialized to the raw byte vector in the cache.
 * Serializing to storage is at least an order of magnitude slower than memory
 * so to ensure adequate performance use a cache with a relatively large
 * allocation of memory and keep object size limits relatively large. This
 * ensures that most objects will be serialized to memory only.
 */
public class LargeObjectArray<T extends Serializable>
{
    // Used to generate unique key for instances.
    private static AtomicLong nextKey = new AtomicLong();
    private static String     keyBase = LargeObjectArray.class.getSimpleName();

    // Backing byte array cache.
    private RawByteCache byteCache;
    private String       key;

    // Array position information.
    private ArrayList<Long> offsets = new ArrayList<Long>();

    /**
     * Creates a new instance with a unique key with cache allocaton.
     * 
     * @param byteCache Backing storage
     */
    public LargeObjectArray(RawByteCache byteCache)
    {
        this.byteCache = byteCache;
        this.key = keyBase + "-" + nextKey.getAndIncrement();
        this.byteCache.allocate(this.key);
    }

    /**
     * Deallocates the array resources.
     */
    public void release()
    {
        if (byteCache != null)
        {
            this.byteCache.deallocate(key);
            this.byteCache = null;
        }
    }

    /**
     * Returns the object key in backing storage.
     */
    public String getKey()
    {
        return this.key;
    }

    /** Returns the backing storage. */
    public RawByteCache getByteCache()
    {
        return this.byteCache;
    }

    /**
     * Adds an object to the end of the array.
     */
    public void add(T element)
    {
        // Note the offset to which we are writing and add to array of object
        // offsets we are tracking.
        offsets.add(byteCache.size(key));

        // Append the serialized object to the cache.
        try
        {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            ObjectOutputStream objectOutput = new ObjectOutputStream(
                    byteOutput);
            objectOutput.writeObject(element);
            objectOutput.flush();
            this.byteCache.append(key, byteOutput.toByteArray());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to write object", e);
        }
    }

    /**
     * Returns a single object from the array.
     */
    public T get(int index)
    {
        InputStream byteInput = null;
        try
        {
            byteInput = byteCache.allocateStream(key);
            long offset = offsets.get(index);
            long skipped = byteInput.skip(offset);
            if (offset != skipped)
                throw new RuntimeException(
                        "Unable to seek to large object array offset: index="
                                + index + " offset=" + offset);
            ObjectInputStream objectInput = new ObjectInputStream(byteInput);
            @SuppressWarnings("unchecked")
            T element = (T) objectInput.readObject();
            return element;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to read object", e);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Unable to read object", e);
        }
        finally
        {
            if (byteInput != null)
            {
                try
                {
                    byteInput.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * Resizes the array to contain a smaller number of elements. Has no effect
     * if the size is greater than current number of elements.
     * 
     * @param size Number of elements
     */
    public void resize(int size)
    {
        // Resizing only has effect if we make the array smaller.
        if (size >= 0 && size < offsets.size())
        {
            // Resize the byte vector in the cache.
            long offset = offsets.get(size);
            byteCache.resize(key, offset);

            // Remove object offsets greater than new size.
            ArrayList<Long> newOffsets = new ArrayList<Long>(size);
            for (int i = 0; i < size; i++)
            {
                newOffsets.add(offsets.get(i));
            }
            offsets = newOffsets;
        }
    }

    /**
     * Returns the number of elements.
     */
    public int size()
    {
        return offsets.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * Returns a scanner on the array.
     */
    public LargeObjectScanner<T> scanner() throws IOException
    {
        return new LargeObjectScanner<T>(this);
    }
}
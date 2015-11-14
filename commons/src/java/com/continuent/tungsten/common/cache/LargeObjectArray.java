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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements an unbounded list that stores objects in the list in serialized
 * form using a RawByteCache that automatically spills to storage to enable
 * unlimited list sizes.
 * </p>
 * Arrays have a buffer size that determines the number of items to hold as
 * normal Java objects. Above this size the array contents spill into cache.
 * Serializing to storage is at least two orders of magnitude slower than memory
 * so to ensure adequate performance use a cache with a relatively large buffer
 * size and keep object size limits relatively large. This ensures that most
 * lists will be handled only as Java objects while extremely large lists will
 * spill to storage.
 */
public class LargeObjectArray<T extends Serializable>
{
    // Used to generate unique key for instances.
    private static AtomicLong nextKey = new AtomicLong();
    private static String     keyBase = LargeObjectArray.class.getSimpleName();

    // Buffer array for objects that are held in memory. If null it means the
    // list is in the byte cache.
    List<T> buffer;
    int     bufferSize;

    // Backing byte array cache.
    private RawByteCache byteCache;
    private String       key;

    // Cursor definition and table of open cursors.
    class ArrayCursor
    {
        long                id;
        private InputStream byteInput;
        private int         index = 0;
    }

    private AtomicLong             nextCursorId = new AtomicLong();
    private Map<Long, ArrayCursor> openCursors  = new HashMap<Long, ArrayCursor>();

    // Array position information.
    private ArrayList<Long> offsets = new ArrayList<Long>();

    /**
     * Creates a new instance with a unique key with cache allocaton.
     * 
     * @param byteCache Backing storage
     * @param bufferSize Number of entries to hold as Java objects before
     *            spilling to cache
     */
    public LargeObjectArray(RawByteCache byteCache, int bufferSize)
    {
        // Set up the cache entry.
        this.byteCache = byteCache;
        this.key = keyBase + "-" + nextKey.getAndIncrement();
        this.byteCache.allocate(this.key);

        // Only allocate a local buffer if the size is positive.
        if (bufferSize > 0)
        {
            this.bufferSize = bufferSize;
            this.buffer = new ArrayList<T>();
        }
    }

    /**
     * Deallocates the array resources.
     */
    public void release()
    {
        if (openCursors != null)
        {
            // Get the cursor IDs.
            List<Long> cursorIds = new ArrayList<Long>(openCursors.size());
            for (long id : openCursors.keySet())
            {
                cursorIds.add(id);
            }
            // Now deallocate by id. This has to be a separate step to avoid
            // confusing the key set iterator.
            for (long id : cursorIds)
            {
                cursorDeallocate(id);
            }

        }

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
        // If we using the local buffer try to add object to it.
        if (buffer != null)
        {
            if (buffer.size() < bufferSize)
            {
                // Buffer has space, so just add.
                buffer.add(element);
            }
            else
            {
                // Buffer is too large, so we need to spill existing values to
                // cache and then add the current element.
                for (T bufferedElement : buffer)
                {
                    this.addToCache(bufferedElement);
                }
                addToCache(element);

                // Remove the buffer so that it is no longer possible to use it.
                buffer = null;
                bufferSize = -1;
            }
        }
        else
        {
            // Add the value to the cache.
            addToCache(element);
        }
    }

    /**
     * Add an element to the byte cache.
     */
    private void addToCache(T element)
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
        if (buffer != null)
        {
            // Values are in the local buffer.
            return buffer.get(index);
        }
        else
        {
            // Values are in cache, so return from there.
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
                ObjectInputStream objectInput = new ObjectInputStream(
                        byteInput);
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
    }

    /**
     * Resizes the array to contain a smaller number of elements. Has no effect
     * if the size is greater than current number of elements.
     * 
     * @param size Number of elements
     */
    public void resize(int size)
    {
        if (buffer != null)
        {
            // Elements are in local buffer. Resize if new size is smaller.
            if (size >= 0 && size < buffer.size())
            {
                int oldLastIndex = buffer.size() - 1;
                int newLastIndex = size - 1;
                for (int i = oldLastIndex; i > newLastIndex; i--)
                {
                    buffer.remove(i);
                }
            }
        }
        else
        {
            // Elements are in cache. Resize if we make the array smaller.
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
    }

    /**
     * Returns the number of elements.
     */
    public int size()
    {
        if (buffer != null)
            return buffer.size();
        else
            return offsets.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * Returns a scanner on the array. This acts like an iterator using cursor
     * operations.
     */
    public LargeObjectScanner<T> scanner() throws IOException
    {
        // Scanner needs to know if data are locally buffered.
        return new LargeObjectScanner<T>(this);
    }

    /**
     * Allocates a new cursor that may be used to read this array and returns
     * the cursor id.
     */
    public long cursorAllocate()
    {
        long cursorId = this.nextCursorId.incrementAndGet();
        ArrayCursor cursor = new ArrayCursor();
        cursor.id = cursorId;
        cursor.index = 0;
        openCursors.put(cursorId, cursor);
        return cursorId;
    }

    /**
     * Returns true if the cursor exists and has at least one element remaining.
     */
    public boolean cursorHasNext(long id)
    {
        return getCursor(id).index < size();
    }

    /**
     * Returns the element at the current cursor position and increments.
     */
    public T cursorNext(long id)
    {
        ArrayCursor cursor = getCursor(id);
        if (cursor.index < size())
        {
            if (buffer != null)
            {
                // Elements are in local buffer. Return the next value.
                return buffer.get(cursor.index++);
            }
            else
            {
                // Elements are in the array cache.
                try
                {
                    // If we don't have an input stream on the cache, allocate
                    // it now.
                    if (cursor.byteInput == null)
                    {
                        cursor.byteInput = getByteCache()
                                .allocateStream(getKey());
                    }

                    // Allocate object input stream to read next serialized
                    // object, which is serialized separately. We don't close
                    // the stream as that would screw up the position of the
                    // underlying byte reader.
                    ObjectInputStream objectInput = new ObjectInputStream(
                            cursor.byteInput);
                    @SuppressWarnings("unchecked")
                    T nextObject = (T) objectInput.readObject();
                    cursor.index++;
                    return nextObject;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(
                            "Unable to deserialize next object: count="
                                    + cursor.index,
                            e);
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException(
                            "Unable to deserialize next object: count="
                                    + cursor.index,
                            e);
                }
            }
        }
        else
            throw new NoSuchElementException("Cursor exceeded array size: size="
                    + size() + " cursor index=" + cursor.index);
    }

    /**
     * Deallocates a cursor. This operation is idempotent.
     */
    public void cursorDeallocate(long id)
    {
        ArrayCursor cursor = openCursors.remove(id);
        if (cursor != null && cursor.byteInput != null)
        {
            try
            {
                cursor.byteInput.close();
            }
            catch (IOException e)
            {
            }
        }
    }

    /**
     * Returns a cursor corresponding to the id argument.
     * 
     * @throws NoSuchElementException Thrown if there is no cursor corresponding
     *             to the id
     */
    private ArrayCursor getCursor(long id) throws NoSuchElementException
    {
        ArrayCursor cursor = openCursors.get(id);
        if (cursor == null)
            throw new NoSuchElementException(
                    "Cursor does not exist or has been deallocated: " + id);
        else
            return cursor;
    }
}
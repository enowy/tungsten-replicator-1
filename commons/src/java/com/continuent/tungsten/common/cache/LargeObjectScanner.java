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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Implements a scan operation that returns all elements of the array in order
 * by deserializing them in sequence.
 */
public class LargeObjectScanner<T extends Serializable> implements AutoCloseable
{
    private final LargeObjectArray<T> array;
    private InputStream               byteInput;
    private int                       count = 0;

    /**
     * Creates a new scanner on anbounded array.
     */
    public LargeObjectScanner(LargeObjectArray<T> array) throws IOException
    {
        this.array = array;
        byteInput = array.getByteCache().allocateStream(array.getKey());
    }

    /**
     * Returns true if there is another object to read.
     */
    public boolean hasNext()
    {
        return count < array.size();
    }

    /**
     * Fetches the next object.
     */
    public T next()
    {
        if (hasNext())
        {
            try
            {
                // Allocate object input stream to read next serialized object,
                // which is serialized separately. We don't close the stream
                // as that would screw up the position of the underlying byte
                // reader.
                ObjectInputStream objectInput = new ObjectInputStream(
                        byteInput);
                @SuppressWarnings("unchecked")
                T nextObject = (T) objectInput.readObject();
                count++;
                return nextObject;
            }
            catch (IOException e)
            {
                throw new RuntimeException(
                        "Unable to deserialize next object: count=" + count, e);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(
                        "Unable to deserialize next object: count=" + count, e);
            }
        }
        else
            throw new NoSuchElementException(
                    "Scanner exceeded array size: count=" + count);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception
    {
        byteInput.close();
    }
}
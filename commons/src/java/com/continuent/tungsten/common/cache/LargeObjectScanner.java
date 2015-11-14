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
import java.io.Serializable;

/**
 * Implements a scan operator that returns all elements of the array.
 */
public class LargeObjectScanner<T extends Serializable> implements AutoCloseable
{
    // The scanner state includes the array itself and a cursor on it.
    private final LargeObjectArray<T> array;
    private final long                cursorId;

    /**
     * Creates a new scanner on the array.
     */
    public LargeObjectScanner(LargeObjectArray<T> array) throws IOException
    {
        this.array = array;
        this.cursorId = array.cursorAllocate();
    }

    /**
     * Returns true if there is another object to read.
     */
    public boolean hasNext()
    {
        return array.cursorHasNext(cursorId);
    }

    /**
     * Fetches the next object.
     */
    public T next()
    {
        return array.cursorNext(cursorId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close()
    {
        array.cursorDeallocate(cursorId);
    }
}
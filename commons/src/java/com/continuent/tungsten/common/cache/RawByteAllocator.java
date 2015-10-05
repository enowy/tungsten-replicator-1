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
import java.util.ArrayList;
import java.util.List;

/**
 * Stores allocation information for cached vectors. We store the head of the
 * byte vector in memory until it exceeds cache limits and then put the
 * remainder in storage.
 */
public class RawByteAllocator
{
    // Unique identifier for this vector.
    String key = null;

    // List of immutable byte buffers written to head of vector and
    // total size of said buffers.
    List<byte[]> buffers      = new ArrayList<byte[]>();
    int          memoryLength = 0;

    // Storage file and bytes stored there.
    File cacheFile     = null;
    long storageLength = 0;

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
        sb.append(" buffers=" + buffers.size());
        sb.append(" memory=" + this.memoryLength);
        sb.append(" storage=" + this.storageLength);
        if (cacheFile != null)
            sb.append(" cacheFile=" + cacheFile.getName());
        return sb.toString();
    }

}
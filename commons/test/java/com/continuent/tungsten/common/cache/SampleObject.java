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

import java.io.Serializable;

/**
 * Sample object used to test object serialization.
 */
public class SampleObject implements Serializable
{
    private static final long serialVersionUID = 1L;
    private final int         i;
    private final String      s;
    private final double      d;

    SampleObject(int i, String s, double d)
    {
        this.i = i;
        this.s = s;
        this.d = d;
    }

    @Override
    public boolean equals(Object o)
    {
        // Assume no fields are null.
        if (!(o instanceof SampleObject))
            return false;
        SampleObject other = (SampleObject) o;
        if (i != other.i)
            return false;
        if (!s.equals(other.s))
            return false;
        if (d != other.d)
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(": i=").append(i);
        sb.append(" s=").append(s);
        sb.append(" d=").append(d);
        return sb.toString();
    }
}
/*
 * Copyright (c) 2012, 2013, 2014
 * Database Research Group, University of Marburg.
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
 */

package de.umr.jepc.engine.correlator.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Basically this class is a wrapper for Object[], but can be printed in a pretty way.
 *
 * @author Marcus Pinnecke
 * @version 1.0
 */
public class RawData {

    /*
     * Data
     */
    private Object[] data;

    /**
     * Constructs a new instance with the content of the data list of <b>payload1</b> followed <b>payload2</b>.
     *
     * @param payload1  Payload instance
     * @param payload2  Other Payload instance
     */
    public RawData(RawData payload1, RawData payload2) {
        List l = new ArrayList<>(Arrays.asList(payload1.data));
        for(Object e : payload2.data)
            l.add(e);
        data = l.toArray(new Object[l.size()]);
    }

    /**
     * @return The underlying object array of this raw data object
     */
    public Object[] getData() {
        return data;
    }

    /**
     * Constructs a new instance with the given array of objects.
     * @param data Data
     */
    public RawData(Object... data) {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException();

        this.data = data;
    }

    /**
     * Gets the data at column <b>index</b>, zero indexed.
     * @param index Index
     * @return The data stored at position <b>index</b>
     */
    public Object get(int index) {
        return data[index];
    }

    @Override
    public String toString() {
        return "RawObject{" +
                "data=" + Arrays.toString(data) +
                '}';
    }
}

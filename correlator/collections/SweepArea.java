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

package de.umr.jepc.engine.correlator.collections;

import de.umr.jepc.engine.correlator.stream.EventObject;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * A sweep area is a data structure to hold dynamic elements of a given type and it's used for stateful streaming
 * operators.
 *
 * @param <PayloadType> Type of data to hold inside the sweep area
 * @author Marcus Pinnecke
 */
public interface SweepArea<PayloadType> {

    /**
     * Add a new event with payload type <code>PayloadType</code> into this sweep area.
     *
     * @param event Event to insert
     */
    public void add(EventObject<PayloadType> event);

    /**
     * Returns the current size of this sweep area.
     * @return Size of this collection.
     */
    public int size();

    /**
     * Clears this sweep area by removing all elements and resetting all internal fields.
     */
    public void clear();

    /**
     * Removes each event which end timestamp is less or equal to <b>timestamp</b>
     */
    public void removeWithEndTimeStampLQ(long timestamp);

    /**
     * Returns an iterator over all events which start timestamp is less (not equal) to <b>timestamp</b>.
     *
     * @param timestamp Upper bound of query.
     * @return Iterator for events where event.startTime < <b>timestamp</b>.
     */
    public Iterator<EventObject<PayloadType>> queryStartTimestampLess(long timestamp);

    /**
     * Returns the current maximal start timestamp inside this SweepArea.
     *
     * @return Maximum start timestamp.
     */
    public long getStartMaxTimestamp();
}

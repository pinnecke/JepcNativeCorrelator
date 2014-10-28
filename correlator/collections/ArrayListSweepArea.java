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

import de.umr.jepc.engine.correlator.utils.ComparatorFactory;
import de.umr.jepc.engine.correlator.stream.EventObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Implementation of <code>SweepArea</code> with <code>ArrayList</code> and Java 1.8 stream usage.
 *
 * @author Marcus Pinnecke
 * @param <PayloadType>
 */
public class ArrayListSweepArea<PayloadType> implements SweepArea<PayloadType> {

    private List<EventObject<PayloadType>> content = new ArrayList<>();
    private long maxStartTimestamp = Long.MIN_VALUE;

    public ArrayListSweepArea() {
        initializeSweepArea();
    }

    private void initializeSweepArea() {
        this.content.clear();
        this.maxStartTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void add(EventObject<PayloadType> event) {
        if (event.getTimeSpan().getStart() > maxStartTimestamp)
            maxStartTimestamp = event.getTimeSpan().getStart();
        content.add(event);
    }

    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void clear() {
        initializeSweepArea();
    }

    @Override
    public void removeWithEndTimeStampLQ(long timestamp) {
        content.removeIf( it -> it.getTimeSpan().getEnd() <= timestamp);
    }

    @Override
    public Iterator<EventObject<PayloadType>> queryStartTimestampLess(long timestamp) {
        return content.stream().filter(it -> it.getTimeSpan().getStart() < timestamp).iterator();
    }

    @Override
    public long getStartMaxTimestamp() {
        return maxStartTimestamp;
    }

}

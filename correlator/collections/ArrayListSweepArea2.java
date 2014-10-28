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
import de.umr.jepc.engine.correlator.utils.ComparatorFactory;

import java.util.*;

/**
 * Implementation of <code>SweepArea</code> with <code>ArrayList</code> and Java 1.8 stream usage.
 *
 * @author Marcus Pinnecke
 * @param <PayloadType>
 */
public class ArrayListSweepArea2<PayloadType> implements SweepArea<PayloadType> {

    private SortedSet<EventObject<PayloadType>> contentByEndTimestamp = new TreeSet<EventObject<PayloadType>>(ComparatorFactory.getInstance().eventByTimeSpanEnd());
    private List<EventObject<PayloadType>> contentByStartTimestamp = new ArrayList<>();

    private long maxStartTimestamp = Long.MIN_VALUE;
    private List<EventObject<PayloadType>> queryResult = new ArrayList<>();

    public ArrayListSweepArea2() {
        initializeSweepArea();
    }

    private void initializeSweepArea() {
        this.contentByEndTimestamp.clear();
        this.contentByStartTimestamp.clear();
        this.maxStartTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void add(EventObject<PayloadType> event) {
        if (event.getTimeSpan().getStart() > maxStartTimestamp)
            maxStartTimestamp = event.getTimeSpan().getStart();
        contentByStartTimestamp.add(event);
        contentByEndTimestamp.add(event);
    }

    @Override
    public int size() {
        return contentByEndTimestamp.size();
    }

    @Override
    public void clear() {
        initializeSweepArea();
    }

    @Override
    public void removeWithEndTimeStampLQ(long timestamp) {
        while (!contentByEndTimestamp.isEmpty() && contentByEndTimestamp.first().getTimeSpan().getEnd() <= timestamp) {
            contentByStartTimestamp.remove(contentByEndTimestamp.first());
            contentByEndTimestamp.remove(contentByEndTimestamp.first());
        }
    }

    @Override
    public Iterator<EventObject<PayloadType>> queryStartTimestampLess(long timestamp) {
        queryResult.clear();
        for (int i = 0; i < contentByStartTimestamp.size(); i++) {
            if (contentByStartTimestamp.get(i).getTimeSpan().getStart() < timestamp)
                queryResult.add(contentByStartTimestamp.get(i));
            else break;
        }
        return queryResult.iterator();
    }

    @Override
    public long getStartMaxTimestamp() {
        return maxStartTimestamp;
    }

}

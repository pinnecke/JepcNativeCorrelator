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

import de.umr.jepc.engine.correlator.utils.ComparatorFactory;

import java.util.Comparator;

/**
 * @author Marcus Pinnecke
 */
public final class EventObject<PayloadType> {

    private PayloadType payload;
    private TimeSpan timeSpan;

    public EventObject(final PayloadType payload, final TimeSpan timeSpan) {
        this.payload = payload;
        this.timeSpan = timeSpan;
    }

    public PayloadType getPayload() {
        return payload;
    }

    public TimeSpan getTimeSpan() {
        return timeSpan;
    }

    @Override
    public String toString() {
        return "EventObject{" +
                "payload=" + payload +
                ", timeSpan=" + timeSpan +
                '}';
    }

    public static class EventTimeSpanStartTimeComparator<PayloadType> implements Comparator<EventObject<PayloadType>> {

        @Override
        public int compare(EventObject<PayloadType> o1, EventObject<PayloadType> o2) {
            TimeSpan.StartTimestampComparator comparator = ComparatorFactory.getInstance().timeSpanByStart();
            return comparator.compare(o1.getTimeSpan(), o2.getTimeSpan());
        }

    }

    public static class EventTimeSpanEndTimeComparator<PayloadType>  implements Comparator<EventObject<PayloadType>> {

        @Override
        public int compare(EventObject<PayloadType> o1, EventObject<PayloadType> o2) {
            TimeSpan.EndTimestampComparator comparator = ComparatorFactory.getInstance().timeSpanByEnd();
            return comparator.compare(o1.getTimeSpan(), o2.getTimeSpan());
        }

    }


    public static class EventTimeSpanStartTimeThenEndTimeComparator<PayloadType> implements Comparator<EventObject<PayloadType>> {

        @Override
        public int compare(EventObject<PayloadType> o1, EventObject<PayloadType> o2) {
            TimeSpan.StartTimestampComparator comparatorStart = ComparatorFactory.getInstance().timeSpanByStart();
            TimeSpan.EndTimestampComparator comparatorEnd = ComparatorFactory.getInstance().timeSpanByEnd();
            int startCompare = comparatorStart.compare(o1.getTimeSpan(), o2.getTimeSpan());
            if (startCompare != 0)
                return startCompare;
            else return comparatorEnd.compare(o1.getTimeSpan(), o2.getTimeSpan());
        }

    }
}

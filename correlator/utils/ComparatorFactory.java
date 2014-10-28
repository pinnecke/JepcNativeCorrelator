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

package de.umr.jepc.engine.correlator.utils;

import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.TimeSpan;

import java.util.Comparator;

/**
 * Factory for comparators
 * @author Marcus Pinnecke
 */
public class ComparatorFactory {

    private static ComparatorFactory instance;

    private ComparatorFactory() {}

    public static ComparatorFactory getInstance() {
        if (instance == null)
            instance = new ComparatorFactory();
        return instance;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private TimeSpan.StartTimestampComparator timeSpanStartTimestampComparatorInstance = new TimeSpan.StartTimestampComparator();
    private TimeSpan.EndTimestampComparator timeSpanEndTimestampComparatorInstance = new TimeSpan.EndTimestampComparator();
    private EventObject.EventTimeSpanStartTimeComparator eventTimeSpanStartTimestampComparatorInstance = new EventObject.EventTimeSpanStartTimeComparator();
    private EventObject.EventTimeSpanEndTimeComparator eventTimeSpanEndTimestampComparatorInstance = new EventObject.EventTimeSpanEndTimeComparator();
    private EventObject.EventTimeSpanStartTimeThenEndTimeComparator eventTimeSpanStartTimeThenEndTimeComparator = new EventObject.EventTimeSpanStartTimeThenEndTimeComparator();

    public TimeSpan.StartTimestampComparator timeSpanByStart() {
        return timeSpanStartTimestampComparatorInstance;
    }

    public EventObject.EventTimeSpanStartTimeComparator eventByTimeSpanStart() {
        return eventTimeSpanStartTimestampComparatorInstance;
    }


    public EventObject.EventTimeSpanEndTimeComparator eventByTimeSpanEnd() {
        return eventTimeSpanEndTimestampComparatorInstance;
    }

    public TimeSpan.EndTimestampComparator timeSpanByEnd() {
        return timeSpanEndTimestampComparatorInstance;
    }

    public EventObject.EventTimeSpanStartTimeThenEndTimeComparator eventByTimeSpanStartThenTimeSpanEnd() {
        return eventTimeSpanStartTimeThenEndTimeComparator;
    }
}

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

import java.util.Comparator;

/**
 * This class models a time span for data events by a left closed right open interval.
 *
 * @author Marcus Pinnecke
 * @version 1.0
 */
public class TimeSpan {

    /*
     * Lower and upper bound
     */
    long start, end = 0;

    @Override
    public String toString() {
        return "TimeSpan {" +
                "start=" + start +
                ", end=" + end +
                '}';
    }

    /**
     * Returns the lower bound of this time interval.
     * @return Start time
     */
    public long getStart() {
        return start;
    }

    /**
     * Returns the upper bound of this time interval.
     * @return End time
     */
    public long getEnd() {
        return end;
    }

    /**
     * Constructs a new timestamp with given lower and upper bound. Please ensure, that <b>start</b> is less
     * than <b>end</b> value. Otherwise an <code>IllegalArgumentException</code> will be thrown.
     *
     * @param start Lower bound
     * @param end Upper bound
     */
    public TimeSpan(long start, long end) {
        if (start >= end)
            throw new IllegalArgumentException("Illegal time interval detected (start="+start+", end="+end+").");

        this.start = start;
        this.end = end;
    }

    /**
     * Computes the intersection of two given time spans <b>left</b> and <b>right</b> by
     * <code><pre>
         long start = Math.max(left.start, right.start);
         long end = Math.min(left.end, right.end);
     * </pre></code>
     * @param left  time span one
     * @param right time span two
     * @return A new time span instance which models the intersection of <b>left</b> and <b>right</b>
     */
    public static TimeSpan intersection(final TimeSpan left, final TimeSpan right) {
        long start = Math.max(left.start, right.start);
        long end = Math.min(left.end, right.end);
        return new TimeSpan(start, end);
    }

    /**
     * Comparator for time spans which natural increasing orders time spans by their start timestamps.
     *
     * @author Marcus Pinnecke
     */
    public static class StartTimestampComparator implements Comparator<TimeSpan> {
        @Override public int compare(TimeSpan o1, TimeSpan o2) {
            return o1.start == o2.start? 0 : o1.start < o2.start? -1 : 1;
        }
    }

    /**
     * Comparator for time spans which natural increasing orders time spans by their end timestamps.
     *
     * @author Marcus Pinnecke
     */
    public static class EndTimestampComparator implements Comparator<TimeSpan> {
        @Override public int compare(TimeSpan o1, TimeSpan o2) {
            return o1.end == o2.end? 0 : o1.end < o2.end? -1 : 1;
        }
    }
}

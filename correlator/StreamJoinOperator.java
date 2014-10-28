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

package de.umr.jepc.engine.correlator;

import de.umr.jepc.engine.correlator.collections.ArrayListSweepArea;
import de.umr.jepc.engine.correlator.collections.SweepArea;
import de.umr.jepc.engine.correlator.stream.EventObject;

import java.util.Iterator;
import java.util.function.*;

/**
 * Implementation of a native binary join over data streams for JEPC. For conceptual issues see
 * Jürgen Krämer. Continuous Queries over Data Streams – Semantics and Implementation.
 * <br><br><b>Note</b><br/>
 * Please note, that this implementation is generic over the two input stream channel types
 * (<b>PayloadTypeLeft</b>, <b>PayloadTypeRight</b>) and over the output result type (<b>PayloadTypeResult</b>)
 *
 * <br><br><b>Example</b><br>The following example constructs two data streams and pushes
 * into the left resp. the right operator stream channel. With a given join predicate (equality of tuples)
 * the join merges the events and outputs them to the console.
 * <code><pre>
     NativeCorrelator<RawData, RawData, RawData> correlator = new NativeCorrelator<>(
     (lhs, rhs) -> lhs.getPayload().get(0).equals(rhs.getPayload().get(0)),
     (lhs, rhs) -> new EventObject<>(new RawData(lhs.getPayload(), rhs.getPayload()), TimeSpan.intersection(lhs.getTimeSpan(), rhs.getTimeSpan())),
     () -> new ArrayListSweepArea<>(), () -> new ArrayListSweepArea<>());

     correlator.addConsumer(result -> System.out.println(result));

     correlator.pushLeft(new EventObject<>(new RawData('c'), new TimeSpan(1, 8)));
     correlator.pushLeft(new EventObject<>(new RawData('a'), new TimeSpan(5, 11)));
     correlator.pushLeft(new EventObject<>(new RawData('d'), new TimeSpan(6, 14)));
     correlator.pushLeft(new EventObject<>(new RawData('a'), new TimeSpan(9, 10)));
     correlator.pushLeft(new EventObject<>(new RawData('b'), new TimeSpan(12, 17)));

     correlator.pushRight(new EventObject<>(new RawData('b'), new TimeSpan(1, 7)));
     correlator.pushRight(new EventObject<>(new RawData('d'), new TimeSpan(3, 9)));
     correlator.pushRight(new EventObject<>(new RawData('a'), new TimeSpan(4, 5)));
     correlator.pushRight(new EventObject<>(new RawData('b'), new TimeSpan(7, 15)));
     correlator.pushRight(new EventObject<>(new RawData('e'), new TimeSpan(10, 18)));

 </pre></code>
 The result is
 <code><pre>
     EventObject{payload=RawObject{data=[d, d]}, timeSpan=TimeSpan {start=6, end=9}}
     EventObject{payload=RawObject{data=[b, b]}, timeSpan=TimeSpan {start=12, end=15}}
 </pre></code>
 *
 * @author Marcus Pinnecke
 * @version 1.0
 */
public class StreamJoinOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult> extends StreamOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult> {

    /*
     * Predicate which determines if a pair of given events should be joined. This predicate is user defined
     * and given by the constructor.
     */
    protected BiPredicate<EventObject<PayloadTypeLeft>, EventObject<PayloadTypeRight>> joinPredicate = null;

    /*
     * If a pair of two given events satisfies {@link #joinPredicate} then the following function is applied to
     * them. The result is stored inside {@link outputHeap} to ensure the natural order of time for output
     * events. Please note that both input data types and also the resulting type could differ.
     */
    private BiFunction<EventObject<PayloadTypeLeft>, EventObject<PayloadTypeRight>, EventObject<PayloadTypeResult>> joinFunction = null;

    /*
     * Two temporal data structures to hold the dynamic and temporal event data.
     * {@link SweepArea}
     */
    private SweepArea<PayloadTypeLeft> statusLeft = new ArrayListSweepArea<>();
    private SweepArea<PayloadTypeRight> statusRight = new ArrayListSweepArea<>();

    public StreamJoinOperator(final BiPredicate<EventObject<PayloadTypeLeft>, EventObject<PayloadTypeRight>> joinPredicate,
                              final BiFunction<EventObject<PayloadTypeLeft>, EventObject<PayloadTypeRight>, EventObject<PayloadTypeResult>> joinFunction,
                              final Supplier<SweepArea<PayloadTypeLeft>> temporalCollectionSupplierLeft,
                              final Supplier<SweepArea<PayloadTypeRight>> temporalCollectionSupplierRight) {
        if (joinFunction == null || temporalCollectionSupplierLeft == null ||
                temporalCollectionSupplierRight == null)
            throw new IllegalArgumentException();

        this.joinPredicate = joinPredicate;
        this.joinFunction = joinFunction;
        this.statusLeft = temporalCollectionSupplierLeft.get();
        this.statusRight = temporalCollectionSupplierRight.get();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  Implementation of IBinaryOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult>
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected void performPushLeft(EventObject<PayloadTypeLeft> event) {
        push(statusLeft, statusRight, event);
        flush();
    }

    @Override
    protected void performPushRight(EventObject<PayloadTypeRight> event) {
        push(statusRight, statusLeft, event);
        flush();
    }

    /**
     * Calculates the join over data streams according to
     * Jürgen Krämer. Continuous Queries over Data Streams – Semantics and Implementation
     * Page 78.
     *
     * @param status        Sweep Area One
     * @param otherStatus   Sweep Area Two
     * @param event         Element to push into <b>otherStatus</b>
     */
    private void push(SweepArea status, SweepArea otherStatus, EventObject event) {
        status.removeWithEndTimeStampLQ(event.getTimeSpan().getStart());
        Iterator<EventObject> iterator = status.queryStartTimestampLess(event.getTimeSpan().getEnd());
        while (iterator.hasNext()) {
            EventObject potentialPartner = iterator.next();
                if (joinPredicate.test(event, potentialPartner))
                    super.addOutput(joinFunction.apply(event, potentialPartner));
        }
        if (event.getTimeSpan().getEnd() > otherStatus.getStartMaxTimestamp())
            otherStatus.add(event);
    }

}

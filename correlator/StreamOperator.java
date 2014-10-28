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

import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.utils.ComparatorFactory;
import de.umr.jepc.engine.correlator.utils.TimeOrderCheck;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.collections.queues.Heap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Abstract base class for binary data stream operators which automatically checks the increasing order of input
 * events of both input channels and implements the management of subscribers.
 *
 * @param <PayloadTypeLeft> Payload type for left input channel
 * @param <PayloadTypeRight> Payload type for right input channel
 * @param <PayloadTypeResult> Payload type for results
 *
 * @author Marcus Pinnecke
 */
public abstract class StreamOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult> implements IStreamOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult> {

    private List<Consumer<EventObject<PayloadTypeResult>>> resultConsumers = new ArrayList<>();
    private TimeOrderCheck timeOrderCheckLeft = new TimeOrderCheck();
    private TimeOrderCheck timeOrderCheckRight = new TimeOrderCheck();
    private long lastStartTimestampLeftChannel = Long.MAX_VALUE;
    private long lastStartTimestampRightChannel = Long.MAX_VALUE;

    /*
     * Output heap to ensure the increasing order over start time for output events
     */
    private Heap<EventObject<PayloadTypeResult>> outputHeap = new DynamicHeap<EventObject<PayloadTypeResult>>(ComparatorFactory.getInstance().eventByTimeSpanStartThenTimeSpanEnd());

    protected abstract void performPushLeft(EventObject<PayloadTypeLeft> event);
    protected abstract void performPushRight(EventObject<PayloadTypeRight> event);


    public final void flush() {
        while(!outputHeap.isEmpty() && outputHeap.peek().getTimeSpan().getStart() <= Math.min(lastStartTimestampLeftChannel, lastStartTimestampRightChannel))
            notifyConsumers(outputHeap.dequeue());
    }

    public final void addOutput(EventObject<PayloadTypeResult> result) {
        outputHeap.enqueue(result);
    }

    @Override
    public final void pushLeft(EventObject<PayloadTypeLeft> event) {
        timeOrderCheckLeft.check(event.getTimeSpan().getStart());
        lastStartTimestampLeftChannel = event.getTimeSpan().getStart();
        performPushLeft(event);
    }

    @Override
    public final void pushRight(EventObject<PayloadTypeRight> event) {
        timeOrderCheckRight.check(event.getTimeSpan().getStart());
        lastStartTimestampRightChannel = event.getTimeSpan().getStart();
        performPushRight(event);
    }

    @Override
    public final void addConsumer(Consumer<EventObject<PayloadTypeResult>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.add(consumer);
    }

    @Override
    public final void removeConsumer(Consumer<EventObject<PayloadTypeResult>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.remove(consumer);
    }

    @Override
    public final void removeAllConsumers(Collection<Consumer<EventObject<PayloadTypeResult>>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.removeAll(consumer);
    }

    @Override
    public final void hasConsumer(Consumer<EventObject<PayloadTypeResult>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.contains(consumer);
    }

    @Override
    public final boolean hasAllConsumers(Collection<Consumer<EventObject<PayloadTypeResult>>> consumers) {
        if (consumers == null)
            throw new IllegalArgumentException();
        else return resultConsumers.containsAll(consumers);
    }

    @Override
    public final Collection<Consumer<EventObject<PayloadTypeResult>>> getConsumers() {
        return resultConsumers;
    }

    protected final void notifyConsumers(final EventObject<PayloadTypeResult> output) {
        resultConsumers.forEach(consumer -> consumer.accept(output));
    }

}

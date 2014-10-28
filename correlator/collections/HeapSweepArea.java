package de.umr.jepc.engine.correlator.collections;

import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.utils.ComparatorFactory;
import xxl.core.collections.queues.DynamicHeap;
import xxl.core.collections.queues.Heap;

import java.util.*;

/**
 * Created by marcus on 08.10.14.
 */
public class HeapSweepArea<PayloadType> implements SweepArea<PayloadType> {

    private Heap<EventObject<PayloadType>> contentByStartTimestamp = new DynamicHeap<EventObject<PayloadType>>(ComparatorFactory.getInstance().eventByTimeSpanStart());
    private Heap<EventObject<PayloadType>> contentByEndTimestamp = new DynamicHeap<EventObject<PayloadType>>(ComparatorFactory.getInstance().eventByTimeSpanEnd());

    private long maxStartTimestamp = Long.MIN_VALUE;

    public HeapSweepArea() {
        initializeSweepArea();
    }

    private void initializeSweepArea() {
        this.contentByStartTimestamp.clear();
        this.contentByEndTimestamp.clear();
        this.maxStartTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void add(EventObject<PayloadType> event) {
        if (event.getTimeSpan().getStart() > maxStartTimestamp)
            maxStartTimestamp = event.getTimeSpan().getStart();
        contentByStartTimestamp.enqueue(event);
        contentByEndTimestamp.enqueue(event);
    }

    @Override
    public int size() {
        if (contentByStartTimestamp.size() != contentByEndTimestamp.size())
            throw new InternalError();
        return contentByEndTimestamp.size();
    }

    @Override
    public void clear() {
        contentByStartTimestamp.clear();
        contentByEndTimestamp.clear();
    }

    Set<EventObject<PayloadType>> removedItems = new HashSet<>();

    @Override
    public void removeWithEndTimeStampLQ(long timestamp) {
        while (!contentByEndTimestamp.isEmpty() && contentByEndTimestamp.peek().getTimeSpan().getEnd() <= timestamp) {
            EventObject<PayloadType> nextEvent = contentByEndTimestamp.dequeue();
            removedItems.add(nextEvent);
        }
    }

    @Override
    public Iterator<EventObject<PayloadType>> queryStartTimestampLess(long timestamp) {
        List<EventObject<PayloadType>> result = new ArrayList<>();
        while (!contentByStartTimestamp.isEmpty() && contentByStartTimestamp.peek().getTimeSpan().getStart() < timestamp) {
            EventObject<PayloadType> nextEvent = contentByStartTimestamp.dequeue();
            if (!removedItems.contains(nextEvent)) {
                result.add(nextEvent);
            }
        }
        for (EventObject<PayloadType> e : result)
            contentByStartTimestamp.enqueue(e);
        return result.iterator();
    }

    @Override
    public long getStartMaxTimestamp() {
        return maxStartTimestamp;
    }

}

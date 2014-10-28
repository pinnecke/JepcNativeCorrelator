package de.umr.jepc.engine.correlator.collections;

import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.utils.ComparatorFactory;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by marcus on 08.10.14.
 */
public class TreeSetSweepArea<PayloadType> implements SweepArea<PayloadType> {

    private SortedSet<EventObject<PayloadType>> contentByStartTimestamp = new TreeSet<EventObject<PayloadType>>(ComparatorFactory.getInstance().eventByTimeSpanStart());
    private SortedSet<EventObject<PayloadType>> contentByEndTimestamp = new TreeSet<EventObject<PayloadType>>(ComparatorFactory.getInstance().eventByTimeSpanEnd());

    private long maxStartTimestamp = Long.MIN_VALUE;

    public TreeSetSweepArea() {
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
        contentByStartTimestamp.add(event);
        contentByEndTimestamp.add(event);
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

    @Override
    public void removeWithEndTimeStampLQ(long timestamp) {
        while (!contentByEndTimestamp.isEmpty() && contentByEndTimestamp.first().getTimeSpan().getEnd() <= timestamp) {
            EventObject<PayloadType> nextEvent = contentByEndTimestamp.first();
            contentByEndTimestamp.remove(nextEvent);
            contentByStartTimestamp.remove(nextEvent);
        }
    }

    @Override
    public Iterator<EventObject<PayloadType>> queryStartTimestampLess(long timestamp) {
        List<EventObject<PayloadType>> result = new ArrayList<>();
        while (!contentByStartTimestamp.isEmpty() && contentByStartTimestamp.first().getTimeSpan().getStart() < timestamp) {
            EventObject<PayloadType> nextEvent = contentByStartTimestamp.first();
            contentByStartTimestamp.remove(nextEvent);
            result.add(nextEvent);
        }
        contentByStartTimestamp.addAll(result);
        return result.iterator();
    }

    @Override
    public long getStartMaxTimestamp() {
        return maxStartTimestamp;
    }

}

package de.umr.jepc.engine.correlator.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marcus Pinnecke
 */
public final class InputChannel<PayloadType> implements EventStream<PayloadType> {

    private List<Consumer<EventObject<PayloadType>>> resultConsumers = new ArrayList<>();

    public final void push(final PayloadType payload, final TimeSpan timespan) {
        notifyConsumers(new EventObject<PayloadType>(payload, timespan));
    }

    public final void push(final PayloadType payload, final long startTimestamp, final long endTimestamp) {
        push(payload, new TimeSpan(startTimestamp, endTimestamp));
    }

    public final void push(final PayloadType payload, final long startTimestamp) {
        push(payload, startTimestamp, startTimestamp + 1);
    }

    @Override
    public final void addConsumer(Consumer<EventObject<PayloadType>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.add(consumer);
    }

    @Override
    public final void removeConsumer(Consumer<EventObject<PayloadType>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.remove(consumer);
    }

    @Override
    public final void removeAllConsumers(Collection<Consumer<EventObject<PayloadType>>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.removeAll(consumer);
    }

    @Override
    public final void hasConsumer(Consumer<EventObject<PayloadType>> consumer) {
        if (consumer == null)
            throw new IllegalArgumentException();
        else resultConsumers.contains(consumer);
    }

    @Override
    public final boolean hasAllConsumers(Collection<Consumer<EventObject<PayloadType>>> consumers) {
        if (consumers == null)
            throw new IllegalArgumentException();
        else return resultConsumers.containsAll(consumers);
    }

    @Override
    public final Collection<Consumer<EventObject<PayloadType>>> getConsumers() {
        return resultConsumers;
    }

    private final void notifyConsumers(final EventObject<PayloadType> output) {
        resultConsumers.forEach(consumer -> consumer.accept(output));
    }
}

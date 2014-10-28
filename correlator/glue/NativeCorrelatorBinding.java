package de.umr.jepc.engine.correlator.glue;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.OutputProcessor;
import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.InputChannel;
import de.umr.jepc.epa.EPA;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;
import de.umr.jepc.epa.parameter.predicate.Equal;
import de.umr.jepc.store.EventStore;
import de.umr.jepc.util.enums.CapacityUnit;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by marcus on 09.10.14.
 */
public class NativeCorrelatorBinding implements EPProvider  {

    private StreamChannelRegister streamChannelRegister = new StreamChannelRegister();

    private NativeCorrelatorOperator operator;

    public NativeCorrelatorBinding(final String streamNameLeft, final Attribute[] streamSchemaLeft,
                                   final String streamReferenceLeft, final String streamNameRight,
                                   final Attribute[] streamSchemaRight, final String streamReferenceRight,
                                   final BooleanExpression joinCondition) {
        final InputChannel<Object[]> leftSource = new InputChannel<>();
        final InputChannel<Object[]> rightSource = new InputChannel<>();

        operator = new NativeCorrelatorOperator(
                new EventSource<>(leftSource, streamSchemaLeft, streamNameLeft, streamReferenceLeft ),
                new EventSource<>(rightSource, streamSchemaRight, streamNameRight, streamReferenceRight ),
                joinCondition);

        streamChannelRegister.put(streamNameLeft, leftSource);
        streamChannelRegister.put(streamNameRight, rightSource);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////
    ////    EPProvider interface implementation
    ////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void registerStream(String stream, Attribute[] schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerStream(String stream, Attribute attribute, Attribute... attributes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRelation(String relation, Attribute[] schema) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createQuery(EPA query) {

    }

    @Override
    public long updateQuery(EPA query) {
        return 0;
    }

    @Override
    public long getClock(String query) {
        return 0;
    }

    @Override
    public void setFilterSink(String query, long version, long switchTime) {

    }

    @Override
    public void addOutputProcessor(EPA query, OutputProcessor outputProcessor) {

    }

    @Override
    public void pushEvent(String stream, Object[] event) {

    }

    @Override
    public void pushEvent(String stream, Object[] payload, long timestamp) {

    }

    @Override
    public void pushEvent(String stream, Object[] payload, long startTimestamp, long endTimestamp) {

    }

    @Override
    public void insertIntoRelation(String relation, Object[] tuple) {

    }

    @Override
    public void deleteFromRelation(String relation, Object[] tuple) {

    }

    @Override
    public void unregisterStream(String stream) {

    }

    @Override
    public void destroyQuery(EPA query) {

    }

    @Override
    public void removeOutputProcessor(OutputProcessor outputProcessor) {

    }

    @Override
    public void removeOutputProcessor(String stream, OutputProcessor outputProcessor) {

    }

    @Override
    public EventStore getEventStore() {
        return null;
    }

    @Override
    public void setEventStoreEnabled(boolean enabled) {

    }

    @Override
    public boolean isEventStoreEnabled() {
        return false;
    }

    @Override
    public void setEventStoreCapacity(int capacity, CapacityUnit capacityUnit) {

    }

    @Override
    public Attribute[] getOutputSchema(EPA query) {
        return new Attribute[0];
    }
}

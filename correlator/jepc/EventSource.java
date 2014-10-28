package de.umr.jepc.engine.correlator.jepc;

import de.umr.jepc.Attribute;
import de.umr.jepc.engine.correlator.stream.EventStream;

/**
 * @author Marcus Pinnecke
 */
public final class EventSource<PayloadType> {

        private EventStream<PayloadType> eventStream;
        private Attribute[] eventStreamSchema;
        private String streamIdentifier;
        private String streamReferenceIdentifier;

        public EventSource(EventStream<PayloadType> eventStream, Attribute[] eventStreamSchema, String streamIdentifier, String streamReferenceIdentifier) {
            this.eventStream = eventStream;
            this.eventStreamSchema = eventStreamSchema;
            this.streamIdentifier = streamIdentifier;
            this.streamReferenceIdentifier = streamReferenceIdentifier;
        }

        public EventStream<PayloadType> getEventStream() {
            return eventStream;
        }

        public Attribute[] getEventStreamSchema() {
            return eventStreamSchema;
        }

        public String getStreamIdentifier() {
            return streamIdentifier;
        }

        public String getStreamReferenceIdentifier() {
            return streamReferenceIdentifier;
        }
}

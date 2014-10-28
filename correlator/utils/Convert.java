package de.umr.jepc.engine.correlator.utils;

import de.umr.jepc.engine.correlator.stream.EventObject;

/**
 * Created by marcus on 09.10.14.
 */
public class Convert {

    public static Object[] toArray(EventObject<Object[]> eventObject) {
        Object[] convertedEvent = new Object[eventObject.getPayload().length + 2];
        System.arraycopy(eventObject.getPayload(), 0, convertedEvent, 0, eventObject.getPayload().length);
        convertedEvent[convertedEvent.length-2] = eventObject.getTimeSpan().getStart();
        convertedEvent[convertedEvent.length-1] = eventObject.getTimeSpan().getEnd();
        return convertedEvent;
    }

}

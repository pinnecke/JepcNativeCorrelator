package de.umr.jepc.engine.correlator.demo;

import de.umr.jepc.engine.correlator.*;
import de.umr.jepc.engine.correlator.collections.ArrayListSweepArea;
import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.RawData;
import de.umr.jepc.engine.correlator.stream.TimeSpan;

/**
 * @author Marcus Pinnecke
 */
public class JoinDemo {

    public static void main(String[] args) {

        StreamJoinOperator<RawData, RawData, RawData> correlator = new StreamJoinOperator<>(
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


    }
}

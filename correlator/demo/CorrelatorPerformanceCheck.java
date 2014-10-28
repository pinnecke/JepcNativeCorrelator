package de.umr.jepc.engine.correlator.demo;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.bridges.WebMethodsEngine;
import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.InputChannel;
import de.umr.jepc.epa.Correlator;
import de.umr.jepc.epa.Stream;
import de.umr.jepc.epa.parameter.predicate.Equal;

/**
 * @author Marcus Pinnecke
 */
public class CorrelatorPerformanceCheck {

    final static int N = 10000 * 15;

    public static void main(String[] args) {

        testRTM();
        testNC();


    }

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    private static void testRTM() {
        for (int n = 1; n < N; n += 10000) {

            EPProvider rtm = new_WebMethodsEngine();
            rtm.setEventStoreEnabled(false);
            rtm.registerStream("LEFT_STREAM_NAME", new Attribute("a", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER));
            rtm.registerStream("RIGHT_STREAM_NAME", new Attribute("b", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER));
            rtm.createQuery(new Correlator("Cor", new Stream("LEFT_STREAM_NAME"), "LHS", new Stream("RIGHT_STREAM_NAME"), "RHS", new Equal("LHS.a", "RHS.b")));

            int offset = 0;
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < n; i++) {
                offset = 18 * i;
                rtm.pushEvent("LEFT_STREAM_NAME", new Object[]{"c", 1}, offset+1, offset+8);
                rtm.pushEvent("LEFT_STREAM_NAME", new Object[]{"a", 1}, offset+5, offset+11);
                rtm.pushEvent("LEFT_STREAM_NAME", new Object[]{"d", 2}, offset+6, offset+14);
                rtm.pushEvent("LEFT_STREAM_NAME", new Object[]{"a", 2}, offset+9, offset+10);
                rtm.pushEvent("LEFT_STREAM_NAME", new Object[]{"b", 3}, offset+12, offset+17);
            }

            rtm.pushEvent("RIGHT_STREAM_NAME", new Object[]{"b", 1}, 1, offset+7);
            rtm.pushEvent("RIGHT_STREAM_NAME", new Object[]{"d", 2}, 3, offset+9);
            rtm.pushEvent("RIGHT_STREAM_NAME", new Object[]{"a", 3}, 4, offset+5);
            rtm.pushEvent("RIGHT_STREAM_NAME", new Object[]{"b", 4}, 7, offset+15);
            rtm.pushEvent("RIGHT_STREAM_NAME", new Object[]{"e", 5}, 10, offset+18);

            long endTime = System.currentTimeMillis() - startTime;

            int eventCount = 5 + n*5;

            System.out.println("RTM;" + eventCount + ";" + eventCount/(endTime/1000f));

        }
    }

    private static void testNC() {
        for (int n = 1; n < N; n += 5000) {

            InputChannel<Object[]> leftSource = new InputChannel<>();
            InputChannel<Object[]> rightSource = new InputChannel<>();

            NativeCorrelatorOperator nc = new NativeCorrelatorOperator(new EventSource<>(leftSource, new Attribute[]{new Attribute("a", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER)}, "LEFT_STREAM_NAME", "LHS"),
                    new EventSource<>(rightSource, new Attribute[]{new Attribute("b", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER)}, "RGHT_STREAM_NAME", "RHS"),
                    new Equal("LHS.a", "RHS.b"));

            int offset = 0;
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < n; i++) {
                offset = 18 * i;
                leftSource.push(new Object[]{"c", 1}, offset+1, offset+8);
                leftSource.push(new Object[]{"a", 1}, offset+5, offset+11);
                leftSource.push(new Object[]{"d", 2}, offset+6, offset+14);
                leftSource.push(new Object[]{"a", 2}, offset+9, offset+10);
                leftSource.push(new Object[]{"b", 3}, offset+12, offset+17);
            }

            rightSource.push(new Object[]{"b", 1}, 1, offset+7);
            rightSource.push(new Object[]{"d", 2}, 3, offset+9);
            rightSource.push(new Object[]{"a", 3}, 4, offset+5);
            rightSource.push(new Object[]{"b", 4}, 7, offset+15);
            rightSource.push(new Object[]{"e", 5}, 10, offset+18);

            long endTime = System.currentTimeMillis() - startTime;

            int eventCount = 5 + n*5;

            System.out.println("NC;" + eventCount + ";" + eventCount/(endTime/1000f));

        }
    }
}

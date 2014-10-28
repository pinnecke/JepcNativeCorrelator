package de.umr.jepc.engine.correlator.demo;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.OutputProcessor;
import de.umr.jepc.bridges.WebMethodsEngine;
import de.umr.jepc.engine.NativeEngine;
import de.umr.jepc.engine.correlator.StreamJoinOperator;
import de.umr.jepc.engine.correlator.collections.ArrayListSweepArea;
import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.RawData;
import de.umr.jepc.engine.correlator.stream.TimeSpan;
import de.umr.jepc.epa.Correlator;
import de.umr.jepc.epa.Stream;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;
import de.umr.jepc.epa.parameter.booleanexpression.True;
import de.umr.jepc.epa.parameter.predicate.Equal;
import de.umr.jepc.epa.parameter.predicate.GreaterEqual;
import de.umr.jepc.epa.parameter.window.CountWindow;
import de.umr.jepc.epa.parameter.window.TimeWindow;
import de.umr.jepc.epa.parameter.window.Window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Marcus Pinnecke
 */
public class NativeEngineDemo {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    public static void main(String[] args) {
        List<Object[]> list1 = testRun(new TimeWindow(10), new TimeWindow(1));
        List<Object[]> list2 = testRun(new TimeWindow(100), new TimeWindow(1));
        List<Object[]> list3 = testRun(new TimeWindow(1000), new TimeWindow(1));
        List<Object[]> list4 = testRun(new TimeWindow(10000), new TimeWindow(1));
        List<Object[]> list5 = testRun(new TimeWindow(1), new TimeWindow(1));
        List<Object[]> list6 = testRun(new TimeWindow(10), new TimeWindow(10));
        List<Object[]> list7 = testRun(new TimeWindow(100), new TimeWindow(100));
        List<Object[]> list8 = testRun(new TimeWindow(1000), new TimeWindow(1000));
        List<Object[]> list9 = testRun(new TimeWindow(10000), new TimeWindow(10000));
        List<Object[]> list19 = testRun(new TimeWindow(1000), new CountWindow(10000));
        List<Object[]> list20 = testRun(new TimeWindow(100), new CountWindow(10000));
        List<Object[]> list21 = testRun(new TimeWindow(10), new CountWindow(10000));
        List<Object[]> list22 = testRun(new TimeWindow(1), new CountWindow(10000));
        List<Object[]> list23 = testRun(new TimeWindow(1), new CountWindow(1000));
        List<Object[]> list24 = testRun(new TimeWindow(1), new CountWindow(100));
        List<Object[]> list25 = testRun(new TimeWindow(1), new CountWindow(10));
        List<Object[]> list26 = testRun(new TimeWindow(1), new CountWindow(1));
        List<Object[]> list27 = testRun(new CountWindow(1), new TimeWindow(1));
        List<Object[]> list28 = testRun(new CountWindow(10), new TimeWindow(1));
        List<Object[]> list29 = testRun(new CountWindow(100), new TimeWindow(1));
        List<Object[]> list39 = testRun(new CountWindow(100), new TimeWindow(100));
        List<Object[]> list40 = testRun(new CountWindow(10), new TimeWindow(10));
        List<Object[]> list41 = testRun(new CountWindow(1), new TimeWindow(1));


    }
    
    public static List<Object[]> testRun(Window window1, Window window2) {
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new GreaterEqual("LHS.ia", "RHS.ia");

        EPProvider nativeEngine = new_WebMethodsEngine();// new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, window1), refLeft, new Stream(stream2, window2), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                //System.out.println("Out: " + Arrays.toString(event));
                outputNE.add(event);
            }
        });

        Random random = new Random(23);

        int offset = 0;
        for (int i = 0; i < 100; i++) {
            nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, offset + 1);
            nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, offset + 5);
            nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, offset + 6);
            nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, offset + 9);
            nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, offset + 12);

            nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, offset + 1);
            nativeEngine.pushEvent("Stream2", new Object[]{"d", 100}, offset + 3);
            nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, offset + 4);
            nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, offset + 7);
            nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, offset + 10);

            offset += 100;

            // The lines above doesn't change the result set, so try a little bit more random generation
            for (int k = 0; k < random.nextInt(10_000); k++) {
                final int timespanMax1 = 1 + random.nextInt(10_000);

                nativeEngine.pushEvent("Stream1", new Object[]{"c", random.nextInt(3)}, offset + timespanMax1);
                nativeEngine.pushEvent("Stream2", new Object[]{"b", random.nextInt(3)}, offset + 1);
                offset += 2 + timespanMax1 + random.nextInt(100);
            }
        }

        System.out.println("Done");
        
        return outputNE;
    }
}

package de.umr.jepc.engine.correlator.demo;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.OutputProcessor;
import de.umr.jepc.bridges.WebMethodsEngine;
import de.umr.jepc.engine.NativeEngine;
import de.umr.jepc.epa.Correlator;
import de.umr.jepc.epa.Stream;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;
import de.umr.jepc.epa.parameter.booleanexpression.True;
import de.umr.jepc.epa.parameter.predicate.GreaterEqual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by marcus on 28.10.14.
 */
public class TestRTMArragement {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    public static void main(String[] args) {
        testPushEventOnly(new_WebMethodsEngine(), "RTM");
        testPushEventTimestamp(new_WebMethodsEngine(), "RTM");
        testPushEventStartEnd(new_WebMethodsEngine(), "RTM");

        testPushEventOnly(new NativeEngine(), "NativeEngine");
        testPushEventTimestamp(new NativeEngine(), "NativeEngine");
        testPushEventStartEnd(new NativeEngine(), "NativeEngine");
    }

    private static void testPushEventStartEnd(EPProvider engine, String s) {
        System.out.println(s + ": testPushEventOnly");
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final Attribute[] schemaStream2 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new True();

        engine.registerStream(stream1, schemaStream1);
        engine.registerStream(stream2, schemaStream2);
        engine.createQuery(new Correlator("Cor", new Stream(stream1), refLeft, new Stream(stream2), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        engine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("Out: " + Arrays.toString(event));
            }
        });


        engine.pushEvent("Stream1", new Object[]{"a"}, 1L,2L);
        engine.pushEvent("Stream1", new Object[]{"b"}, 2L,3L);
        engine.pushEvent("Stream2", new Object[]{"A"}, 1L,2L);
        engine.pushEvent("Stream2", new Object[]{"B"}, 2L,3L);
    }

    private static void testPushEventTimestamp(EPProvider engine, String s) {
        System.out.println(s + ": testPushEventOnly");
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final Attribute[] schemaStream2 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new True();

        engine.registerStream(stream1, schemaStream1);
        engine.registerStream(stream2, schemaStream2);
        engine.createQuery(new Correlator("Cor", new Stream(stream1), refLeft, new Stream(stream2), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        engine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("Out: " + Arrays.toString(event));
            }
        });


        engine.pushEvent("Stream1", new Object[]{"a"}, 1L);
        engine.pushEvent("Stream1", new Object[]{"b"}, 2L);
        engine.pushEvent("Stream2", new Object[]{"A"}, 1L);
        engine.pushEvent("Stream2", new Object[]{"B"}, 2L);
    }

    private static void testPushEventOnly(EPProvider engine, String s) {
        System.out.println(s + ": testPushEventOnly");
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final Attribute[] schemaStream2 = new Attribute[]{new Attribute("sa", Attribute.DataType.STRING)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new True();

        engine.registerStream(stream1, schemaStream1);
        engine.registerStream(stream2, schemaStream2);
        engine.createQuery(new Correlator("Cor", new Stream(stream1), refLeft, new Stream(stream2), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        engine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("Out: " + Arrays.toString(event));
            }
        });


        engine.pushEvent("Stream1", new Object[]{"a", 1L,2L});
        engine.pushEvent("Stream1", new Object[]{"b", 2L,3L});
        engine.pushEvent("Stream2", new Object[]{"A", 1L,2L});
        engine.pushEvent("Stream2", new Object[]{"B", 2L,3L});

    }
}

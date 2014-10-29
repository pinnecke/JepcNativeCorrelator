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
import de.umr.jepc.epa.parameter.predicate.LessEqual;
import de.umr.jepc.epa.parameter.window.CountWindow;
import de.umr.jepc.epa.parameter.window.TimeWindow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by marcus on 28.10.14.
 */
public class TestJoinCondition {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    public static void main(String[] args) {
        testPushEventOnly(new_WebMethodsEngine(), "RTM");
        testPushEventOnly(new NativeEngine(), "NativeEngine");
    }


    private static void testPushEventOnly(EPProvider engine, String s) {
        System.out.println(s + ": testPushEventOnly");
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[]{new Attribute("i", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[]{new Attribute("i", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new LessEqual("LHS.i", "RHS.i");

        engine.registerStream(stream1, schemaStream1);
        engine.registerStream(stream2, schemaStream2);
        engine.createQuery(new Correlator("Cor", new Stream(stream1), refLeft, new Stream(stream2, new CountWindow(5)), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        engine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("Out: " + Arrays.toString(event));
            }
        });


        engine.pushEvent("Stream1", new Object[]{100, 1L,2L});
        engine.pushEvent("Stream1", new Object[]{2, 2L,3L});
        engine.pushEvent("Stream1", new Object[]{3, 3L,4L});
        engine.pushEvent("Stream1", new Object[]{4, 4L,5L});
        engine.pushEvent("Stream1", new Object[]{5, 5L,6L});

        engine.pushEvent("Stream2", new Object[]{2, 1L,2L});
        engine.pushEvent("Stream2", new Object[]{4, 2L,3L});
        engine.pushEvent("Stream2", new Object[]{8, 3L,4L});
        engine.pushEvent("Stream2", new Object[]{16, 3L,5L});
        engine.pushEvent("Stream2", new Object[]{32, 3L,6L});

    }
}

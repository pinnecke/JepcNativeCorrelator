package de.umr.jepc.testing.correlator;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.OutputProcessor;
import de.umr.jepc.bridges.WebMethodsEngine;
import de.umr.jepc.engine.NativeEngine;
import de.umr.jepc.epa.Correlator;
import de.umr.jepc.epa.Stream;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;
import de.umr.jepc.epa.parameter.predicate.*;
import de.umr.jepc.epa.parameter.window.TimeWindow;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marcus Pinnecke
 */
public class OutputComparedToRTMWithWindowStartEndTimestamp {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    @Test
    public void testEqualPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Equal("RHS.sa", "a");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testBetweenPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Between("LHS.ia", 2, 3);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Greater("LHS.ia", 1);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterEqualPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new GreaterEqual("LHS.ia", 1);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Less("LHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessEqualPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new LessEqual("LHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testUnequalPredicateFixedValue() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Unequal("LHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testEqualPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Equal("RHS.sa", "a");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testBetweenPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Between("RHS.ia", 2, 3);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Greater("RHS.ia", 1);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterEqualPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new GreaterEqual("RHS.ia", 1);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Less("RHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessEqualPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new LessEqual("RHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testUnequalPredicateFixedValue2() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Unequal("RHS.ia", 5);

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testEqualPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Equal("LHS.sa", "RHS.sa");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testBetweenPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Between("LHS.ia", 2, "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Greater("LHS.ia", "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testGreaterEqualPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new GreaterEqual("LHS.ia", "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Less("LHS.ia", "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testLessEqualPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new LessEqual("LHS.ia", "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }

    @Test
    public void testUnequalPredicateReference() {

        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new Unequal("LHS.ia", "RHS.ia");

        /**
         * JEPC
         */
        EPProvider rtm = new_WebMethodsEngine();
        rtm.registerStream(stream1, schemaStream1);
        rtm.registerStream(stream2, schemaStream2);
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputRTM = new ArrayList<>();

        rtm.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("RTM: " + Arrays.toString(event));
                outputRTM.add(event);
            }
        });

        /**
         * Native Correlator
         */

        EPProvider nativeEngine = new NativeEngine();
        nativeEngine.registerStream(stream1, schemaStream1);
        nativeEngine.registerStream(stream2, schemaStream2);
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(10)), refLeft, new Stream(stream2, new TimeWindow(10)), refRight, joinCondition));

        List<Object[]> outputNativeCorrelator = new ArrayList<>();

        nativeEngine.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                System.out.println("NE: " + Arrays.toString(event));
                outputNativeCorrelator.add(event);
            }
        });

        /**
         * Pushing data
         */

        // RTM
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1, 8);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1, 8);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5, 11);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6, 14);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9, 10);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12, 17);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3, 9);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4, 5);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7, 15);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10, 18);

        if (outputRTM.isEmpty())
            Assert.fail("No output");

        Assert.assertEquals(outputNativeCorrelator.size(), outputRTM.size());
        for (Object[] event : outputRTM) {
            boolean found = false;
            for (Object[] other : outputNativeCorrelator) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : outputNativeCorrelator) {
            boolean found = false;
            for (Object[] other : outputRTM) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Found " + Arrays.toString(event) + " in NC output, but it shouldn't be here!");
        }

    }



}

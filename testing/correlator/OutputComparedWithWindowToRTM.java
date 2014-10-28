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
import de.umr.jepc.epa.parameter.window.CountWindow;
import de.umr.jepc.epa.parameter.window.TimeWindow;
import de.umr.jepc.epa.parameter.window.Window;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Marcus Pinnecke
 */
public class OutputComparedWithWindowToRTM {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    private static void deepEquals(List<Object[]> left, List<Object[]> right) {
        Assert.assertEquals(left.size(), right.size());
        System.out.println("Equal size for result sets: " + left.size());
        System.out.println("Testing, that output is not empty...");
        Assert.assertEquals(left.size() == 0, false);
        System.out.println("Deep testing of "+(2*((long)left.size()*(long)left.size()))+" items...");
        for (Object[] event : right) {
            boolean found = false;
            for (Object[] other : left) {
                for (int i = 0; i < event.length; i++) {
                    if (!event[i].equals(other[i]))
                        break;
                    found = true;
                }
            }
            if (!found)
                Assert.fail("Expected " + Arrays.toString(event) + " in NC output, but it was not found!");
        }

        for (Object[] event : left) {
            boolean found = false;
            for (Object[] other : right) {
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
    public void testIntensive1() {
        List<Object[]> list1_NE = testRun(new NativeEngine(), new TimeWindow(10), new TimeWindow(1));
        List<Object[]> list1_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(10), new TimeWindow(1));
        deepEquals(list1_NE,list1_RTM);
    }

    @Test
    public void testIntensive2() {
        List<Object[]> list2_NE = testRun(new NativeEngine(), new TimeWindow(100), new TimeWindow(1));
        List<Object[]> list2_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(100), new TimeWindow(1));
        deepEquals(list2_NE,list2_RTM);
    }

    @Test
    public void testIntensive5x() {
        List<Object[]> list5_NE = testRun(new NativeEngine(), new TimeWindow(1), new TimeWindow(1));
        List<Object[]> list5_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(1), new TimeWindow(1));
        deepEquals(list5_NE,list5_RTM);
    }

    @Test
    public void testIntensive5xx() {
        List<Object[]> list5_NE = testRun(new NativeEngine(), new CountWindow(1), new CountWindow(1));
        List<Object[]> list5_RTM = testRun(new_WebMethodsEngine(), new CountWindow(1), new CountWindow(1));
        deepEquals(list5_NE,list5_RTM);
    }

    @Test
    public void testIntensive5xxx() {
        List<Object[]> list5_NE = testRun(new NativeEngine(), new CountWindow(10), new CountWindow(1));
        List<Object[]> list5_RTM = testRun(new_WebMethodsEngine(), new CountWindow(10), new CountWindow(1));
        deepEquals(list5_NE,list5_RTM);
    }

    @Test
    public void testIntensive5xxxx() {
        List<Object[]> list5_NE = testRun(new NativeEngine(), new CountWindow(1), new CountWindow(100));
        List<Object[]> list5_RTM = testRun(new_WebMethodsEngine(), new CountWindow(1), new CountWindow(100));
        deepEquals(list5_NE,list5_RTM);
    }


    @Test
    public void testIntensive6() {
        List<Object[]> list6_NE = testRun(new NativeEngine(), new TimeWindow(10), new TimeWindow(10));
        List<Object[]> list6_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(10), new TimeWindow(10));
        deepEquals(list6_NE,list6_RTM);
    }

    @Test
    public void testIntensive7() {
        List<Object[]> list7_NE = testRun(new NativeEngine(), new TimeWindow(100), new TimeWindow(100));
        List<Object[]> list7_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(100), new TimeWindow(100));
        deepEquals(list7_NE,list7_RTM);
    }

    @Test
    public void testIntensive8() {
        List<Object[]> list8_NE = testRun(new NativeEngine(), new TimeWindow(1000), new TimeWindow(1000));
        List<Object[]> list8_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(1000), new TimeWindow(1000));
        deepEquals(list8_NE,list8_RTM);
    }

    @Test
    public void testIntensive9() {
        List<Object[]> list9_NE = testRun(new NativeEngine(), new TimeWindow(10000), new TimeWindow(10000));
        List<Object[]> list9_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(10000), new TimeWindow(10000));
        deepEquals(list9_NE,list9_RTM);
    }

    @Test
    public void testIntensive15() {
        List<Object[]> list24_NE = testRun(new NativeEngine(), new TimeWindow(1), new CountWindow(10));
        List<Object[]> list24_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(1), new CountWindow(10));
        deepEquals(list24_NE,list24_RTM);
    }

    @Test
    public void testIntensive16() {
        List<Object[]> list25_NE = testRun(new NativeEngine(), new TimeWindow(1), new CountWindow(10));
        List<Object[]> list25_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(1), new CountWindow(10));
        deepEquals(list25_NE,list25_RTM);
    }

    @Test
    public void testIntensive17() {
        List<Object[]> list26_NE = testRun(new NativeEngine(), new TimeWindow(1), new CountWindow(1));
        List<Object[]> list26_RTM = testRun(new_WebMethodsEngine(), new TimeWindow(1), new CountWindow(1));
        deepEquals(list26_NE,list26_RTM);
    }

    @Test
    public void testIntensive18() {
        List<Object[]> list27_NE = testRun(new NativeEngine(), new CountWindow(1), new TimeWindow(1));
        List<Object[]> list27_RTM = testRun(new_WebMethodsEngine(), new CountWindow(1), new TimeWindow(1));
        deepEquals(list27_NE,list27_RTM);
    }

    @Test
    public void testIntensive19() {
        List<Object[]> list28_NE = testRun(new NativeEngine(), new CountWindow(10), new TimeWindow(1));
        List<Object[]> list28_RTM = testRun(new_WebMethodsEngine(), new CountWindow(10), new TimeWindow(1));
        deepEquals(list28_NE,list28_RTM);
    }

    @Test
    public void testIntensive20() {
        List<Object[]> list29_NE = testRun(new NativeEngine(), new CountWindow(100), new TimeWindow(1));
        List<Object[]> list29_RTM = testRun(new_WebMethodsEngine(), new CountWindow(100), new TimeWindow(1));
        deepEquals(list29_NE,list29_RTM);
    }

    @Test
    public void testIntensive21() {
        List<Object[]> list39_NE = testRun(new NativeEngine(), new CountWindow(100), new TimeWindow(100));
        List<Object[]> list39_RTM = testRun(new_WebMethodsEngine(), new CountWindow(100), new TimeWindow(100));
        deepEquals(list39_NE,list39_RTM);
    }

    @Test
    public void testIntensive22() {
        List<Object[]> list40_NE = testRun(new NativeEngine(), new CountWindow(10), new TimeWindow(10));
        List<Object[]> list40_RTM = testRun(new_WebMethodsEngine(), new CountWindow(10), new TimeWindow(10));
        deepEquals(list40_NE,list40_RTM);
    }

    @Test
    public void testIntensive23() {
        List<Object[]> list41_NE = testRun(new NativeEngine(), new CountWindow(1), new TimeWindow(1));
        List<Object[]> list41_RTM = testRun(new_WebMethodsEngine(), new CountWindow(1), new TimeWindow(1));
        deepEquals(list41_NE,list41_RTM);
    }

    public static List<Object[]> testRun(EPProvider epProvider, Window window1, Window window2) {
        final String stream1 = "Stream1";
        final String stream2 = "Stream2";
        final Attribute[] schemaStream1 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final Attribute[] schemaStream2 = new Attribute[] {new Attribute("sa", Attribute.DataType.STRING), new Attribute("ia", Attribute.DataType.INTEGER)};
        final String refLeft = "LHS";
        final String refRight = "RHS";
        final BooleanExpression joinCondition = new GreaterEqual("LHS.ia", "RHS.ia");

        epProvider.registerStream(stream1, schemaStream1);
        epProvider.registerStream(stream2, schemaStream2);
        epProvider.createQuery(new Correlator("Cor", new Stream(stream1, window1), refLeft, new Stream(stream2, window2), refRight, joinCondition));

        List<Object[]> outputNE = new ArrayList<>();

        epProvider.addOutputProcessor(new Stream("Cor"), new OutputProcessor() {
            @Override
            public void process(String stream, Object[] event) {
                //System.out.println("Out: " + Arrays.toString(event));
                outputNE.add(event);
            }
        });


        /**
         * Pushing data
         */

        int offset = 0;
        for (int i = 0; i < 100; i++) {
            epProvider.pushEvent("Stream1", new Object[]{"c", 1}, offset+1);
            epProvider.pushEvent("Stream1", new Object[]{"a", 1}, offset+5);
            epProvider.pushEvent("Stream1", new Object[]{"d", 2}, offset+6);
            epProvider.pushEvent("Stream1", new Object[]{"a", 2}, offset+9);
            epProvider.pushEvent("Stream1", new Object[]{"b", 3}, offset+12);

            epProvider.pushEvent("Stream2", new Object[]{"b", 1}, offset+1);
            epProvider.pushEvent("Stream2", new Object[]{"d", 2}, offset+3);
            epProvider.pushEvent("Stream2", new Object[]{"a", 3}, offset+4);
            epProvider.pushEvent("Stream2", new Object[]{"b", 4}, offset+7);
            epProvider.pushEvent("Stream2", new Object[]{"e", 5}, offset+10);
            offset += 12;
        }


        System.out.println("Done");

        return outputNE;
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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);


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
        rtm.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        nativeEngine.createQuery(new Correlator("Cor", new Stream(stream1, new TimeWindow(5)), refLeft, new Stream(stream2, new TimeWindow(5)), refRight, joinCondition));

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
        rtm.pushEvent("Stream1", new Object[] {"c",1}, 1);
        rtm.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        rtm.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        rtm.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        rtm.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        rtm.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        rtm.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        rtm.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        rtm.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        rtm.pushEvent("Stream2", new Object[]{"e", 5}, 10);

        // NativeCorrelator
        nativeEngine.pushEvent("Stream1", new Object[]{"c", 1}, 1);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 1}, 5);
        nativeEngine.pushEvent("Stream1", new Object[]{"d", 2}, 6);
        nativeEngine.pushEvent("Stream1", new Object[]{"a", 2}, 9);
        nativeEngine.pushEvent("Stream1", new Object[]{"b", 3}, 12);

        nativeEngine.pushEvent("Stream2", new Object[]{"b", 1}, 1);
        nativeEngine.pushEvent("Stream2", new Object[]{"d", 2}, 3);
        nativeEngine.pushEvent("Stream2", new Object[]{"a", 3}, 4);
        nativeEngine.pushEvent("Stream2", new Object[]{"b", 4}, 7);
        nativeEngine.pushEvent("Stream2", new Object[]{"e", 5}, 10);

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

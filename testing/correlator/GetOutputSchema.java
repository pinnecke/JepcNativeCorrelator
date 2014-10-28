package de.umr.jepc.testing.correlator;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.bridges.WebMethodsEngine;
import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.InputChannel;
import de.umr.jepc.epa.Correlator;
import de.umr.jepc.epa.EPA;
import de.umr.jepc.epa.Stream;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;
import de.umr.jepc.epa.parameter.predicate.Equal;
import de.umr.jepc.util.Print;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Marcus Pinnecke
 */
public class GetOutputSchema {

    public static EPProvider new_WebMethodsEngine() {
        return new WebMethodsEngine("JEPC"+System.currentTimeMillis(), "", false);
    }

    @Test
    public void testOutputSchema() {
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
        EPA cor = new Correlator("Cor", new Stream(stream1), refLeft, new Stream(stream2), refRight, joinCondition);
        rtm.createQuery(cor);
        Attribute[] expectedSchema = rtm.getOutputSchema(cor);

        /**
         * Native Correlator
         */

        InputChannel<Object[]> leftSource = new InputChannel<>();
        InputChannel<Object[]> rightSource = new InputChannel<>();

        NativeCorrelatorOperator nc = new NativeCorrelatorOperator(
                new EventSource<>(leftSource, schemaStream1, stream1, refLeft),
                new EventSource<>(rightSource, schemaStream2, stream2, refRight),
                joinCondition);

        Attribute[] actualSchema = nc.getOutputSchema();

        System.out.println("Expected: " + Print.PrintPretty(expectedSchema));
        System.out.println("Actual: " + Print.PrintPretty(actualSchema));

        Assert.assertEquals(actualSchema, expectedSchema);

    }

}

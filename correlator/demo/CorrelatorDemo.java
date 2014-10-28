package de.umr.jepc.engine.correlator.demo;

import de.umr.jepc.Attribute;
import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.InputChannel;
import de.umr.jepc.epa.parameter.predicate.Equal;
import de.umr.jepc.util.Print;

import java.util.Arrays;

/**
 * @author Marcus Pinnecke
 */
public class CorrelatorDemo {

    public static void main(String[] args) {

        InputChannel<Object[]> leftSource = new InputChannel<>();
        InputChannel<Object[]> rightSource = new InputChannel<>();

        NativeCorrelatorOperator nc = new NativeCorrelatorOperator(new EventSource<>(leftSource, new Attribute[] { new Attribute("a", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER) }, "LEFT_STREAM_NAME", "LHS" ),
                                                   new EventSource<>(rightSource, new Attribute[] { new Attribute("b", Attribute.DataType.STRING), new Attribute("c", Attribute.DataType.INTEGER) }, "RGHT_STREAM_NAME", "RHS" ),
                                                   new Equal("LHS.a", "RHS.b"));

        nc.addConsumer( result -> System.out.println(Arrays.toString(result.getPayload())) );

        leftSource.push(new Object[] {'c',1}, 1, 8);
        leftSource.push(new Object[] {'a',1}, 5, 11);
        leftSource.push(new Object[] {'d',2}, 6, 14);
        leftSource.push(new Object[] {'a',2}, 9, 10);
        leftSource.push(new Object[] {'b',3}, 12, 17);

        rightSource.push(new Object[] {'b',1}, 1, 7);
        rightSource.push(new Object[] {'d',2}, 3, 9);
        rightSource.push(new Object[] {'a',3}, 4, 5);
        rightSource.push(new Object[] {'b',4}, 7, 15);
        rightSource.push(new Object[] {'e',5}, 10, 18);


        System.out.println(Print.PrintPretty(nc.getOutputSchema()));

    }
}

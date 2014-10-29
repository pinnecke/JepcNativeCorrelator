package de.umr.jepc.engine.correlator;

import de.umr.jepc.Attribute;
import de.umr.jepc.engine.correlator.collections.*;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.TimeSpan;
import de.umr.jepc.epa.parameter.booleanexpression.BooleanExpression;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * @author Marcus Pinnecke
 */
public class NativeCorrelatorOperator extends StreamJoinOperator<Object[], Object[], Object[]> {

    private static final String ID_DELIMITER = "_";
    private static final String SCHEMA_START_TIME_NAME = "tstart";
    private static final String SCHEMA_END_TIME_NAME = "tend";

    private Attribute[] outputSchema;

    private Map<String, Object> assignmentMap = new HashMap<>();

    public NativeCorrelatorOperator(final EventSource<Object[]> lhs, final EventSource<Object[]> rhs, final BooleanExpression joinCondition) {
        super(null, createJoinFunction(), () -> new ArrayListSweepArea2<>(), () -> new ArrayListSweepArea2<>());
        super.joinPredicate = createJoinPredicate(lhs, rhs, joinCondition);

        lhs.getEventStream().addConsumer( event -> super.pushLeft(event) );
        rhs.getEventStream().addConsumer( event -> super.pushRight(event) );

        outputSchema = createOutputSchema(lhs,rhs);
    }

    private static Attribute[] createOutputSchema(EventSource<Object[]> lhs, EventSource<Object[]> rhs) {
        final String leftId = lhs.getStreamReferenceIdentifier();
        final String rightId = rhs.getStreamReferenceIdentifier();
        Attribute[] leftSchema = removeDuplicates(lhs.getEventStreamSchema());
        Attribute[] rightSchema = removeDuplicates(rhs.getEventStreamSchema());
        Attribute[] outputSchema = new Attribute[leftSchema.length + rightSchema.length + 2];
        for(int i = 0; i < leftSchema.length; i++)
            outputSchema[i] = new Attribute(leftId + ID_DELIMITER + leftSchema[i].getAttributeName().toLowerCase(), leftSchema[i].getAttributeType());

        for(int i = 0; i < rightSchema.length; i++)
                outputSchema[leftSchema.length + i] = new Attribute(rightId + ID_DELIMITER + rightSchema[i].getAttributeName().toLowerCase(), rightSchema[i].getAttributeType());

        outputSchema[outputSchema.length - 2] = new Attribute(SCHEMA_START_TIME_NAME, Attribute.DataType.LONG);
        outputSchema[outputSchema.length - 1] = new Attribute(SCHEMA_END_TIME_NAME, Attribute.DataType.LONG);

        return outputSchema;
    }

    private static Attribute[] removeDuplicates(Attribute[] schema) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute a : schema)
            if (!result.contains(a))
                result.add(a);
        return result.toArray(new Attribute[result.size()]);
    }

    private static BiFunction<EventObject<Object[]>, EventObject<Object[]>, EventObject<Object[]>> createJoinFunction() {
        return (lhs, rhs) -> new EventObject<>(de.umr.jepc.engine.correlator.utils.Arrays.append(lhs.getPayload(), rhs.getPayload()), TimeSpan.intersection(lhs.getTimeSpan(), rhs.getTimeSpan()));
    }

    private BiPredicate<EventObject<Object[]>, EventObject<Object[]>> createJoinPredicate(EventSource<Object[]> leftStreamChannel, EventSource<Object[]> rightStreamChannel, BooleanExpression joinCondition) {
       Attribute[] out = createOutputSchema(leftStreamChannel, rightStreamChannel);

        return (lhs, rhs) -> {
            Object[] payloadLeft = lhs.getPayload();
            Object[] payloadRight = rhs.getPayload();
            for (int i = 0; i < payloadLeft.length; i++) {
                assignmentMap.put(out[i].getAttributeName(), payloadLeft[i]);
            }
            assignmentMap.put(out[payloadLeft.length + 0].getAttributeName(), lhs.getTimeSpan().getStart());
            assignmentMap.put(out[payloadLeft.length + 1].getAttributeName(), lhs.getTimeSpan().getEnd());
            for (int i = 0; i < payloadRight.length; i++) {
                assignmentMap.put(out[payloadLeft.length + 2 + i].getAttributeName(), payloadRight[i]);
            }

            assignmentMap.put(out[payloadRight.length + 2 + payloadLeft.length + 0].getAttributeName(), rhs.getTimeSpan().getStart());
            assignmentMap.put(out[payloadRight.length + 2 + payloadLeft.length + 1].getAttributeName(), rhs.getTimeSpan().getEnd());


            return joinCondition.eval(assignmentMap);
        };
    }

    public Attribute[] getOutputSchema() {
        return outputSchema;
    }
}

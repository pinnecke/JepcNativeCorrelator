package de.umr.jepc.engine.correlator.utils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marcus Pinnecke
 */
public class Evaluator {

    public static boolean evaluate2(final Map<String, Object> assignmentMap, final Object leftExpression, final Object rightExpression,
                                    final BiPredicate<Object, Object> evalFunction) {

        // Output schema of join of streams LHS and RHS is LHS_a, LHS_b, ..., RHS_a', RHS_b',...
        // Although the delimiter inside the resulting schema is a "_" char, the user references
        // for attributes in one of both sides by using a "." char.
        // Example: LHS = (a), RHS = (b), join condition = LHS.a > RHS.b
        //          But the output schema is LHS join RHS = (LHS_a, RHS_b)
        // The following function corrects a possible mismatch between "." and "_" by
        // replacing "." (query form) with "_" (schema form) and it is applied to both expressions for the binary join.
        // The function passes the expression without any changes if it is not a String type
        // Example: join condition = "LHS.a > 23.0f" is converted to "LHS_a > 23.0f"
        Function<Object, Object> correctDot = x -> (x instanceof String)? ((String) x).replace(".", "_") : x;
        Object left = correctDot.apply(leftExpression);
        Object right = correctDot.apply(rightExpression);

        if (left instanceof String && assignmentMap.containsKey(((String) left)) ||
                right instanceof String && assignmentMap.containsKey(((String) right))) {
            // At least one parameter references to an assignment of a given event
            if (left instanceof String && right instanceof String) {
                // Both are string identifiers, switch if a java string compare is intended or
                // a compare of the attribute assignments.
                final String lhs = ((String) left);
                final String rhs = ((String) right);
                if (assignmentMap.containsKey(lhs) && assignmentMap.containsKey(rhs)) {
                    // Reference case
                    return evalFunction.test(assignmentMap.get(lhs), assignmentMap.get(rhs));
                } else if (assignmentMap.containsKey(lhs) && !assignmentMap.containsKey(rhs)) {
                    // Reference compared to string value
                    return evalFunction.test(assignmentMap.get(lhs),rhs);
                } else if (!assignmentMap.containsKey(lhs) && assignmentMap.containsKey(rhs)) {
                    // Reference compared to string value
                    return evalFunction.test(assignmentMap.get(rhs), lhs);
                } else return evalFunction.test(lhs,rhs);
            } else if (left instanceof String && !(right instanceof String)) {
                // Left one could be an reference to the assignment
                final String lhs = (String) left;
                if (assignmentMap.containsKey(lhs)) {
                    return evalFunction.test(assignmentMap.get(lhs), right);
                } else return evalFunction.test(lhs, right);
            } else if (!(left instanceof String) && right instanceof String) {
                // Right one could be an reference to the assignment
                final String rhs = (String) right;
                if (assignmentMap.containsKey(rhs)) {
                    return evalFunction.test(assignmentMap.get(rhs), left);
                } else return evalFunction.test(rhs, left);
            }
        } else {
            // In this case both parameter doesn't reference to the assignment, e.g.
            // Equal(2L, "ABC").
            return evalFunction.test(left,right);
        }
        return false;
    }

}

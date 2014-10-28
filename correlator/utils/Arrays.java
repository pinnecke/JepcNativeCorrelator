package de.umr.jepc.engine.correlator.utils;

/**
 * @author Marcus Pinnecke
 */
public class Arrays {

    public static Object[] append(Object[] lhs, Object[] rhs) {
        Object[] result = new Object[lhs.length + rhs.length];
        for(int i = 0; i < lhs.length; i++)
            result[i] = lhs[i];
        for(int i = 0; i < rhs.length; i++)
            result[lhs.length + i] = rhs[i];
        return result;
    }

}

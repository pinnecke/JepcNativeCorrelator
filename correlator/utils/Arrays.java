package de.umr.jepc.engine.correlator.utils;

/**
 * @author Marcus Pinnecke
 */
public class Arrays {

    public static Object[] append(Object[] lhs, Object[] rhs) {
        Object[] result = new Object[lhs.length + rhs.length];
        System.arraycopy(lhs,0,result,0,lhs.length);
        System.arraycopy(rhs,0,result,lhs.length,lhs.length);
        return result;
    }

}

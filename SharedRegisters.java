package de.umr.jepc.engine;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by marcus on 09.10.14.
 */
public class SharedRegisters {

    /**
     * Needed to give each stream derived from a relation a unique name.
     */
    long relationCount = 0L;

    /**
     * Stores for each relation stream its current clock.
     */
    Map<String,Long> relationstreamToClock = new HashMap<>();

    /**
     * Stores for each EPA its current version.
     */
    Map<String,Long> queryToVersion = new HashMap<>();

    /**
     * Stores for each EPA its switch time
     */
    Map<String,Long> queryToSwitchTime = new HashMap<>();


}

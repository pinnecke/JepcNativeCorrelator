package de.umr.jepc.engine;

import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.InputChannel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Created by marcus on 09.10.14.
 */
public class CorrelatorRegisters {

    /**
     * Stores for each event source all consuming correlators
     * that have a relation as second input: source -> {correlator$version}
     */
    Map<String,Set<String>> sourceToCorrelatorsWithRelation = new HashMap<>();

    /**
     * Stores for each active correlator that as a relation as input
     * the name of the corresponding relation stream: correlator$version -> relationstream
     */
    Map<String,String> correlatorWithRelationToRelationstream = new HashMap<>();

    /**
     * Associate a query name to an instance of NativeCorrelatorOperator
     */
    Map<String, NativeCorrelatorOperator> queryNames2CorrelatorInstances = new HashMap<>();

    /**
     * Associate a stream identifier to an instance of a NativeCorrelatorOperator's InputChannel instance.
     * Each InputChannel instance is connection either as left or as right input stream of a specific
     * NativeCorrelatorOperator instance.
     */
    Map<String, InputChannel<Object[]>> streamNames2CorrelatorInputChannelInstances = new HashMap<>();

    /**
     * Associate a query name (caution: versioned name!) the input channels. This is needed only for a
     * good disposal of the query within DestroyQuery() method. The first (left) input source is indexed with "0"
     * and the second (right) input source is index with "1" inside the query names value.
     */
    public HashMap<String, InputChannel<Object[]>[]> queryNames2CorrelatorInputChannelInstances = new HashMap<>();

    /**
     * Association between an instance of NativeCorrelator and its assigned output processors (which are
     * instances of consumers). This map is needed for deletion of output processors because the
     * subscription of output results of a NativeCorrelator is implemented as an anonymous method call.
     */
    public HashMap<NativeCorrelatorOperator, List<Consumer<EventObject<Object[]>>>> outputProcessors2ConsumerInstances = new HashMap<>();
}

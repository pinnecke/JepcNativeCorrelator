package de.umr.jepc.engine.correlator.glue;


import de.umr.jepc.engine.correlator.stream.InputChannel;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by marcus on 09.10.14.
 */
public class StreamChannelRegister extends HashMap<String, InputChannel<Object[]>> {

    @Override
    public InputChannel<Object[]> put(String key, InputChannel<Object[]> value) {
        if (super.size() >= 2)
            throw new UnsupportedOperationException();
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends InputChannel<Object[]>> m) {
        throw new UnsupportedOperationException();
    }
}

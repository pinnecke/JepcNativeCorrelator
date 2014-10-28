/*
 * Copyright (c) 2012, 2013, 2014
 * Database Research Group, University of Marburg.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.umr.jepc.engine.correlator;

import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.EventStream;

/**
 * Base interface for binary data stream operators. Those operators are defined by a set of subscribers (available
 * as a result of inheritance of <code>EventStream</code>) and the ability to push an event with payload type
 * <b>PayloadTypeLeft</b> into the operators left channel and to push an event with payload type
 * <b>PayloadTypeRight</b> to the operators right channel. The result could differ in type and it set by
 * <b>PayloadTypeResult</b>.
 *
 * @param <PayloadTypeLeft>
 * @param <PayloadTypeRight>
 * @param <PayloadTypeResult>
 *
 * @author Marcus Pinnecke
 */
public interface IStreamOperator<PayloadTypeLeft, PayloadTypeRight, PayloadTypeResult> extends EventStream<PayloadTypeResult> {

    /**
     * Pushes <b>event</b> into the operators left input channel.
     * @param event
     */
    void pushLeft(final EventObject<PayloadTypeLeft> event);

    /**
     * Pushes <b>event</b> into the operators right input channel.
     * @param event
     */
    void pushRight(final EventObject<PayloadTypeRight> event);

}

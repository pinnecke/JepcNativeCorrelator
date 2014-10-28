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

package de.umr.jepc.engine.correlator.stream;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * This interface models the ability of its implementation <b>E</b> that one or more <b>Consumers</b> can subscribe to
 * results of <b>E</b>.
 *
 * @author Marcus Pinnecke
 * @param <PayloadTypeResult> Data type for events
 */
public interface EventStream<PayloadTypeResult> {

    /**
     * Adds a new subscription. <b>consumer</b> will be called if new results are available.
     *
     * @param consumer An instance of Java 1.8 <b>Consumer</b>
     */
    public void addConsumer(final Consumer<EventObject<PayloadTypeResult>> consumer);

    /**
     * Removes a subscription. If <b>consumer</b> was not registered as a consumer before, nothing will happen.
     *
     * @param consumer An instance of Java 1.8 <b>Consumer</b>
     */
    public void removeConsumer(final Consumer<EventObject<PayloadTypeResult>> consumer);

    /**
     * Removes a all subscription by the given collection. If one consumer inside <b>consumers</b> was not registered
     * as a consumer before, nothing will happen.
     *
     * @param consumers An collection of instances of Java 1.8 <b>Consumer</b>
     */
    public void removeAllConsumers(final Collection<Consumer<EventObject<PayloadTypeResult>>> consumers);

    /**
     * Checks if a given <b>consumer</b> was registered as a consumer
     *
     * @param consumer An instance of Java 1.8 <b>Consumer</b>
     */
    public void hasConsumer(final Consumer<EventObject<PayloadTypeResult>> consumer);

    /**
     * Checks if each a given <b>consumer</b> was registered as a consumer
     *
     * @param consumers An collection of instances of Java 1.8 <b>Consumer</b>
     */
    public boolean hasAllConsumers(Collection<Consumer<EventObject<PayloadTypeResult>>> consumers);

    /**
     * Returns all currently registered consumers.
     * @return Collection of registered consumers.
     */
    public Collection<Consumer<EventObject<PayloadTypeResult>>> getConsumers();

}

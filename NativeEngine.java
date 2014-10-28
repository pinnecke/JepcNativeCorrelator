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

package de.umr.jepc.engine;

import java.util.*;
import java.util.function.Consumer;

import de.umr.jepc.Attribute;
import de.umr.jepc.EPProvider;
import de.umr.jepc.OutputProcessor;
import de.umr.jepc.Attribute.DataType;
import de.umr.jepc.engine.aggregatetree.A23Tree;
import de.umr.jepc.engine.betree.BeTree;
import de.umr.jepc.engine.betree.FilterIndex;
import de.umr.jepc.engine.betree.FilterIndexConfiguration;
import de.umr.jepc.engine.betree.FilterIndexFactory;
import de.umr.jepc.engine.correlator.NativeCorrelatorOperator;
import de.umr.jepc.engine.correlator.jepc.EventSource;
import de.umr.jepc.engine.correlator.stream.EventObject;
import de.umr.jepc.engine.correlator.stream.InputChannel;
import de.umr.jepc.engine.correlator.utils.Convert;
import de.umr.jepc.epa.*;
import de.umr.jepc.epa.parameter.aggregate.Aggregate;
import de.umr.jepc.epa.parameter.aggregate.Average;
import de.umr.jepc.epa.parameter.aggregate.Count;
import de.umr.jepc.epa.parameter.aggregate.Group;
import de.umr.jepc.epa.parameter.aggregate.Maximum;
import de.umr.jepc.epa.parameter.aggregate.Minimum;
import de.umr.jepc.epa.parameter.aggregate.Stddev;
import de.umr.jepc.epa.parameter.aggregate.Sum;
import de.umr.jepc.epa.parameter.window.CountWindow;
import de.umr.jepc.epa.parameter.window.PartitionedCountWindow;
import de.umr.jepc.epa.parameter.window.TimeWindow;
import de.umr.jepc.store.EventStore;
import de.umr.jepc.store.memory.MemoryEventStore;
import de.umr.jepc.util.enums.CapacityUnit;

/**
 * Implements a native engine for JEPC. For pattern matching, use
 * TPStream.
 *
 * @author Bastian Hoßbach
 * @author Andreas Morgen
 * @author Stephan Wöllauer
 */
public class NativeEngine implements EPProvider{
	
	/**
	 * Stores for each event source its schema.
	 */
	private Map<String, Attribute[]> source2schema;

   	/**
	 * Stores for each query the corresponding A23Tree
	 */
	private Map<String, A23Tree> query2tree;
	
	// überflüssig?
	private Map<String, String> query2stream;
	
	/**
	 * Stores for each event source the associated queries 
	 */
	private Map<String, Set<String>> source2queries;
	
	/**
	 * Stores for each event source its associated Output Processors 
	 */
	private Map<String, Set<OutputProcessor>> source2outputProcessor;
	
	/**
	 * Stores for each event source its associated windows
	 */
	private Map<String, Set<NativeEngineWindow>> source2windows;
	
    /**
     * Factory for creating new filter indexes.
     */
	private final FilterIndexFactory filterIndexFactory;

    /**
     * Stores for each event source its filter index.
     */
	private final Map<String, FilterIndex> source2FilterIndex;
	
	/**
	 * Stores for each filter its window.
	 */
	private Map<Filter, NativeEngineWindow> filter2window;
	
	/**
	 * The instance of the event store
	 */
	private EventStore eventStore;
	
	/**
	 * Flags if the event store is enabled or not
	 */
	private boolean eventStoreEnabled;
	
    /**
     * Needed to give each window a unique name.
     */
    private long windowCount = 0L;
    
    /**
     * Stores for each EPA its associated windows:
     * query$version -> windows
     */
    private Map<String,List<NativeEngineWindow>> query2windows;

    /**
     * Stores the configuration of filter indexes used in this instance.
     */
    public final FilterIndexConfiguration filterIndexConfiguration;


    /**
     * Contains associations between objects, common for any kind of operator (e.g. relation and stream maps)
     */
    private SharedRegisters sharedRegisters = new SharedRegisters();

    /**
     * Contains associations between objects, only used by Correlator EPA
     */
    private CorrelatorRegisters correlatorRegisters = new CorrelatorRegisters();

    public final static long QUERY_VERSION_ONE = 1L;
    public final static long QUERY_SWITCH_TIME_ZERO = 0L;

    /**
     * Creates a new instance of NativeEngine using the default configuration
     * of filter indexes.
     */
    public NativeEngine() {
    	this(new FilterIndexConfiguration());
    }


    /**
     * Creates a new instance of NativeEngine with user-defined
     * configuration of filter indexes.
     *
     * @param filterIndexConfiguration configuration of filter indexes
     */
    public NativeEngine(FilterIndexConfiguration filterIndexConfiguration) {
    	this.filterIndexConfiguration = filterIndexConfiguration;
		source2schema = new HashMap<String, Attribute[]>();
		query2tree = new HashMap<String, A23Tree>();
		query2stream = new HashMap<String, String>();
		source2queries = new HashMap<String, Set<String>>();
		source2outputProcessor = new HashMap<String,Set<OutputProcessor>>();
		source2windows = new HashMap<String, Set<NativeEngineWindow>>();
		query2windows = new HashMap<String, List<NativeEngineWindow>>();
		filterIndexFactory = new FilterIndexFactory(filterIndexConfiguration);
		source2FilterIndex = new HashMap<String, FilterIndex>();
		filter2window = new HashMap<Filter, NativeEngineWindow>();

		// event store
		try { // btree_store.jar in classpath => Use BTreeEventStore
			Class btEvent = Class.forName("de.umr.jepc.store.btree.TimeSplitBTreeEventStore", false, this.getClass().getClassLoader());
			eventStore = (EventStore) btEvent.newInstance();
		} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e2) {
			eventStore = new MemoryEventStore(); // Fallback => In-memory event store
		}
		eventStore.open();
		eventStoreEnabled = false;
	}

	
	@Override
	public void registerStream(String stream, Attribute[] schema) {
		Attribute[] internalSchema = new Attribute[schema.length+2];
		System.arraycopy(schema, 0, internalSchema, 0, schema.length);
		internalSchema[internalSchema.length-2] = new Attribute("tstart", DataType.LONG);
		internalSchema[internalSchema.length-1] = new Attribute("tend", DataType.LONG);
		source2schema.put(stream, internalSchema);
		source2outputProcessor.put(stream, new HashSet<OutputProcessor>());

	}


	@Override
	public void registerStream(String stream, Attribute attribute, Attribute... attributes) {
		Attribute[] schema = new Attribute[attributes.length+1];
		schema[0] = attribute;
		for(int i=1; i<=attributes.length; i++)
			schema[i] = attributes[i-1];
		registerStream(stream,schema);
	}


    @Override
    public void createQuery(EPA query) {

        Long version = 1L;
        if (query instanceof Aggregator) {
            // Recursive creation of event processing networks - aus RTMEngine
            if (source2schema.get(((Aggregator) query).getSourceName()) == null)
                createQuery(((Aggregator) query).getInputEPA());

            if (((Aggregator) query).getInputEPA() instanceof Stream) {
                String queryName = createWindowedStream((Stream) (((Aggregator) query)).getInputEPA(), query.getName(), version);
                // ordne der Query einen Baum und einen Stream zu
                query2tree.put(queryName, new A23Tree(query.getName(), source2schema.get(((Aggregator) query).getSourceName()), ((Aggregator) query).getAggregat(), this));
                query2stream.put(queryName, ((Aggregator) query).getSourceName());

            }
            registerStream(query.getName(), getSchemaOfAggregator((Aggregator) query));
        } else if (query instanceof Filter) {
            Filter filter = (Filter) query;

            if (source2schema.get(filter.getSourceName()) == null) {
                createQuery(filter.getInputEPA());
            }

            if (filter.getInputEPA() instanceof Stream) {
                Stream stream = (Stream) filter.getInputEPA();

                if (stream.getWindow() instanceof TimeWindow) {

                    Attribute[] filterSchema = source2schema.get(stream.getName());
                    if (filterSchema != null) {
                        FilterIndex filterIndex = source2FilterIndex.get(stream.getName());
                        if (filterIndex == null) {
                            filterIndex = filterIndexFactory.createFilterIndex(filterSchema);
                            source2FilterIndex.put(stream.getName(), filterIndex);
                        }

                        filterIndex.insertFilter(filter);

                        TimeWindow timeWindow = (TimeWindow) stream.getWindow();
                        NativeEngineWindow filterWindow = new NativeEngineWindow(WindowType.TIME_WINDOW, stream.getName(), stream.getWindowSize());
                        filterWindow.initTimeWindow(timeWindow.getJump());
                        filter2window.put(filter, filterWindow);

                        registerStream(filter.getName(), filterSchema);
                    } else {
                        throw new RuntimeException("no input schema");
                    }
                } else {
                    throw new RuntimeException("filter only supports TimeWindow");
                }
            } else {
                throw new RuntimeException("no stream");
            }
        } else if (query instanceof Correlator) {

            createCorrelatorEPA((Correlator) query);

        } else if (query instanceof Stream) {
            throw new RuntimeException("unknown stream: " + query.getName());
        } else {
            throw new RuntimeException("not implemented createQuery: " + query.getClass());
        }
    }

    private void createCorrelatorEPA(Correlator correlator)
    {
        String inputSourceNameLeft = correlator.getSource1Name();
        String inputSourceNameRight = correlator.getSource2Name();
        final EPA inputEPALeft = correlator.getInputEPA1();
        final EPA inputEPARight = correlator.getInputEPA2();
        final String queryName = correlator.getName();

        String inputLeftAccessorName = null;
        String inputRightAccessorName = null;

        createEPNRecursive(inputSourceNameLeft, inputEPALeft);
        createEPNRecursive(inputSourceNameRight, inputEPARight);

        inputLeftAccessorName = buildAccessorName(inputSourceNameLeft, inputEPALeft, queryName, QUERY_VERSION_ONE);
        inputRightAccessorName = buildAccessorName(inputSourceNameRight, inputEPARight, queryName, QUERY_VERSION_ONE);

        if (inputEPALeft instanceof Relation)
            inputLeftAccessorName = inputSourceNameLeft = handleStreamTypeRelation(inputSourceNameLeft,
                    inputSourceNameRight, queryName, QUERY_VERSION_ONE);

        if (inputEPALeft instanceof Relation)
            inputLeftAccessorName = inputSourceNameLeft = handleStreamTypeRelation(inputSourceNameRight,
                    inputSourceNameLeft, queryName, QUERY_VERSION_ONE);

        NativeCorrelatorOperator operator = constructAndRegisterNewNativeCorrelatorOperator(inputSourceNameLeft,
                inputLeftAccessorName, inputSourceNameRight, inputRightAccessorName, correlator, QUERY_VERSION_ONE);

        storeOutputSchemaInformation(operator, queryName);
        storeQueryVersionAndSwitchTime(queryName, QUERY_VERSION_ONE, QUERY_SWITCH_TIME_ZERO);

        activeEventOutputForwarding(operator, queryName);

    }

    private void activeEventOutputForwarding(NativeCorrelatorOperator operator, String queryName) {
        operator.addConsumer(event -> pushEvent(queryName, Convert.toArray(event)));
    }

    private void storeQueryVersionAndSwitchTime(String queryName, long queryVersionOne, long querySwitchTimeZero) {
        sharedRegisters.queryToVersion.put(queryName, queryVersionOne);
        sharedRegisters.queryToSwitchTime.put(queryName, querySwitchTimeZero);
    }

    private void storeOutputSchemaInformation(NativeCorrelatorOperator operator, String queryName) {
        Attribute[] outputSchema = operator.getOutputSchema();
        registerStream (queryName, outputSchema);
        source2schema.put(queryName,outputSchema);
    }

    private NativeCorrelatorOperator constructAndRegisterNewNativeCorrelatorOperator(String inputSourceNameLeft,
                                                                                     String inputLeftAccessorName,
                                                                                     String inputSourceNameRight,
                                                                                     String inputRightAccessorName,
                                                                                     Correlator correlator,
                                                                                     long queryVersion) {
        final InputChannel<Object[]> leftSource = new InputChannel<>();
        final InputChannel<Object[]> rightSource = new InputChannel<>();

        final String queryName = correlator.getName();

        final NativeCorrelatorOperator operator = new NativeCorrelatorOperator(
                new EventSource<>(leftSource, source2schema.get(inputSourceNameLeft), inputLeftAccessorName, correlator.getVar1() ),
                new EventSource<>(rightSource, source2schema.get(inputSourceNameRight), inputRightAccessorName, correlator.getVar2() ),
                correlator.getFormula());

        correlatorRegisters.queryNames2CorrelatorInstances.put (queryName+"$"+queryVersion, operator);
        correlatorRegisters.queryNames2CorrelatorInputChannelInstances.put (queryName+"$"+queryVersion, new InputChannel[] {leftSource, rightSource});
        correlatorRegisters.streamNames2CorrelatorInputChannelInstances.put(inputLeftAccessorName, leftSource);
        correlatorRegisters.streamNames2CorrelatorInputChannelInstances.put(inputRightAccessorName, rightSource);

        return operator;
    }

    private String handleStreamTypeRelation(String inputSourceName1, String inputSourceName2, String queryName, long queryVersion) {
        if(!correlatorRegisters.sourceToCorrelatorsWithRelation.containsKey(inputSourceName2))
            correlatorRegisters.sourceToCorrelatorsWithRelation.put (inputSourceName2,new HashSet<>());

        correlatorRegisters.sourceToCorrelatorsWithRelation.get(inputSourceName2).add(queryName+"$"+queryVersion);
        String resultingRelationName = createRelationStream(inputSourceName1);
        correlatorRegisters.correlatorWithRelationToRelationstream.put(queryName+"$"+queryVersion,resultingRelationName);
        return resultingRelationName;
    }

    private String buildAccessorName(String inputSourceName, EPA inputEPA, String queryName, long queryVersion) {
        if (inputEPA instanceof Stream) {
            return createWindowedStream((Stream) inputEPA, queryName, queryVersion);
        } else {
            return inputSourceName;
        }
    }

    private void createEPNRecursive(String inputSourceName, EPA inputEPA) {
        if (!source2schema.containsKey(inputSourceName))
            createQuery(inputEPA);
    }


    @Override
	public long updateQuery(EPA query) {
		if(query instanceof Filter){
			Filter filter = (Filter) query;
			if(source2schema.get(filter.getSourceName()) == null) {
				updateQuery(filter.getInputEPA());				
			}
			
			if(filter.getInputEPA() instanceof Stream) {
				Stream stream = (Stream) filter.getInputEPA();

				if(stream.getWindow() instanceof TimeWindow) {
					
					FilterIndex filterIndex = source2FilterIndex.get(stream.getName());
					
					if(filterIndex != null) {
						
					} else {
						throw new RuntimeException("updateQuery: filter update only on same stream");
					}
					
					Filter oldFilter = null;
					
					for(Filter keyFilter:filter2window.keySet()) {
						if(keyFilter.getName().equals(filter.getName())) {
							oldFilter = keyFilter;
							break;
						}
					}
					
					if(oldFilter!=null&&filterIndex.deleteFilter(oldFilter)) {
						filterIndex.insertFilter(filter);
						
						TimeWindow timeWindow = (TimeWindow) stream.getWindow();
						NativeEngineWindow filterWindow = new NativeEngineWindow(WindowType.TIME_WINDOW, stream.getName(), stream.getWindowSize());
						filterWindow.initTimeWindow(timeWindow.getJump());
						filter2window.put(filter, filterWindow);
					} else {
						throw new RuntimeException("updateQuery: filter not found");
					}

				} else {
					throw new RuntimeException("filter only supports TimeWindow");
				}
			} else {
				throw new RuntimeException("no stream");
			}
        } else if (query instanceof Correlator) {
            // TODO: Marcus: Implement UpdateQuery in NativeEngine for Correlator
            throw new UnsupportedOperationException("Not implemented yet (for "+query.getClass() + ")");
		} else {
			throw new UnsupportedOperationException("Not implemented yet (for "+query.getClass() + ")");
		}
		return 0; //TODO
	}


	@Override
	public long getClock(String query) {
        throw new UnsupportedOperationException("Not implemented yet.");
	}


	@Override
	public void setFilterSink(String query, long version, long switchTime) {
        throw new UnsupportedOperationException("Not implemented yet.");
	}


	@Override
	public void addOutputProcessor(EPA query, OutputProcessor outputProcessor) {
        source2outputProcessor.get(query.getName()).add(outputProcessor);
        outputProcessor.registerInputStream(query.getName(), source2schema.get(query.getName()));

        if (query instanceof Correlator) {
            NativeCorrelatorOperator operator = correlatorRegisters.queryNames2CorrelatorInstances.get(query.getName());
            Consumer<EventObject<Object[]>> outputConsumer = event -> outputProcessor.process(query.getName(), Convert.toArray(event));
            operator.addConsumer(outputConsumer);
            if (!correlatorRegisters.outputProcessors2ConsumerInstances.containsKey(operator))
                correlatorRegisters.outputProcessors2ConsumerInstances.put(operator, new ArrayList<>());
            correlatorRegisters.outputProcessors2ConsumerInstances.get(operator).add(outputConsumer);
        }
	}


	@Override
	public void pushEvent(String stream, Object[] payload, long timestamp) {
		processPushEventForOthers(stream, payload, timestamp);
        //processPushEventForCorrelator(stream, payload, timestamp);
    }

    private void processPushEventForCorrelator(String stream, Object[] payload, long start, long end) {
        if (correlatorRegisters.streamNames2CorrelatorInputChannelInstances.containsKey(stream)) {
            InputChannel<Object[]> inputChannel = correlatorRegisters.streamNames2CorrelatorInputChannelInstances.get(stream);
            inputChannel.push(payload, start, end);
        }
    }

    private void processPushEventForCorrelator(String stream, Object[] event) {
        System.out.println(stream+";"+event[0]+";"+event[1]+";"+event[2]+";"+event[3]);
        if (correlatorRegisters.streamNames2CorrelatorInputChannelInstances.containsKey(stream)) {
            long start = (long) event[event.length - 2];
            long end = (long) event[event.length - 1];
            Object[] payload = new Object[event.length - 2];
            System.arraycopy(event, 0, payload, 0, payload.length);
            processPushEventForCorrelator(stream, payload, start, end);
        }
    }

    private void processPushEventForOthers(String stream, Object[] payload, long timestamp) {
        Object[] event = new Object[payload.length+2];
        System.arraycopy(payload, 0, event, 0, payload.length);
        event[event.length-2] = timestamp;
        event[event.length-1] = timestamp+1;
        // 1) save event in event store
        if(eventStoreEnabled)
            eventStore.pushEvent(stream, event);
        // 2) push event in all connected OPs
        for(OutputProcessor outputProcessor : source2outputProcessor.get(stream))
            outputProcessor.process(stream, event);
        // 3) push event in windows
        Set<NativeEngineWindow> windows = source2windows.get(stream);
        if(windows != null && !windows.isEmpty()) {
                for (NativeEngineWindow window : windows) {
                    if (query2tree.containsKey(window.e)) {
                        // The following lines handle logic by Andreas for Non-Correlator Handling

                        if (window.type == WindowType.TIME_WINDOW) {
                            Object[] winEvent = window.pushTimeBased(payload, timestamp);
                            if (winEvent != null) {
                                query2tree.get(window.e).insertEvent(winEvent);
                            }
                        } else {
                            List<Object[]> out = window.pushCountBased(payload, timestamp);
                            if (out != null) {
                                for (Object[] winEvent : out)
                                    query2tree.get(window.e).insertEvent(winEvent);
                            }
                        }
                    } else {
                        // Otherwise handle Correlator logic
                        if(window.type == WindowType.TIME_WINDOW) {
                            Object[] winEvent = window.pushTimeBased(payload, timestamp);
                            if(winEvent != null) {
                                processPushEventForCorrelator(window.e, winEvent);
                            }
                        }
                        else {
                            List<Object[]> out;
                            if(window.type == WindowType.COUNT_WINDOW)
                                out = window.pushCountBased(payload, timestamp);
                            else
                                out = window.pushPartitionedCountBased(payload, timestamp);
                            if(out != null) {
                                for(Object[] winEvent : out) {
                                    processPushEventForCorrelator(window.e, winEvent);
                                }
                            }
                        }
                    }

            }
        }
        // 4) push event into filters
        FilterIndex filterIndex = source2FilterIndex.get(stream);
        if(filterIndex!=null) {
            Collection<Filter> resultfilters = filterIndex.checkEvent(payload);
            // push event in output stream of all satisfied filter EPAs
            for(Filter resultFilter:resultfilters) {
                NativeEngineWindow nativeEngineWindow = filter2window.get(resultFilter);
                Object[] windowedEvent = nativeEngineWindow.pushTimeBased(payload, timestamp); //only supports TimeWindow
                pushEvent(resultFilter.getName(),windowedEvent);
            }
        }
    }


    @Override
	public void pushEvent(String stream, Object[] event) {

        // 1) save event in event store
        if(eventStoreEnabled) {
            eventStore.pushEvent(stream, event);
        }
        // 2) push event in all connected OPs
        for(OutputProcessor outputProcessor : source2outputProcessor.get(stream))
            outputProcessor.process(stream, event);
        // 3) update aggregator windows
        if(source2queries.containsKey(stream)) {
            for(String query: source2queries.get(stream))
                query2tree.get(query).insertEvent(event);
        }
        // 4) push event into filters
        FilterIndex filterIndex = source2FilterIndex.get(stream);
        if(filterIndex!=null) {
            Collection<Filter> resultfilters = filterIndex.checkEvent(event);
            // push event in output stream of all satisfied filter EPAs
            for(Filter resultFilter:resultfilters)
        		pushEvent(resultFilter.getName(),event);
        }

        Set<NativeEngineWindow> windows = source2windows.get(stream);
        if(windows != null && !windows.isEmpty()) {
            for(NativeEngineWindow window : windows) {

                processPushEventForCorrelator(window.e, event);



            }
        }

    }

	
	@Override
	public void pushEvent(String stream, Object[] payload, long startTimestamp, long endTimestamp) {
        Object[] event = new Object[payload.length + 2];
        System.arraycopy(payload, 0, event, 0, payload.length);
        event[event.length-2] = startTimestamp;
        event[event.length-1] = endTimestamp;
        pushEvent(stream, event);
    }


	@Override
	public void unregisterStream(String stream) {
		// TODO Auto-generated method stub
	}


	@Override
	public void destroyQuery(EPA query) {
		if(query instanceof Aggregator) {// TODO ??		
			query2tree.remove(query.getName());
			query2stream.remove(query.getName());
		} else if(query instanceof Filter) {
			Filter filter = (Filter) query;
			if(filter.getInputEPA() instanceof Stream) {
				Stream stream = (Stream) filter.getInputEPA();
				FilterIndex filterIndex = source2FilterIndex.get(stream.getName());
				if(filterIndex!=null) {
					if(filterIndex.deleteFilter(filter)) {
						filter2window.remove(filter.getName());
					}
				}
			}
		} else if(query instanceof Correlator) {
            // TODO: Marcus: Implement destroy query for correlator
            throw new UnsupportedOperationException("Not implemented yet");
        }
	}


	@Override
	public void removeOutputProcessor(OutputProcessor outputProcessor) {
		for(String stream : query2tree.keySet())
			removeOutputProcessor(stream, outputProcessor);
		
		for(Set<OutputProcessor> set:source2outputProcessor.values()) {
			set.remove(outputProcessor);
			if(set.isEmpty()) {
				//TODO
			}
		}
	}


	@Override
	public void removeOutputProcessor(String stream, OutputProcessor outputProcessor) {
		source2outputProcessor.get(stream).remove(outputProcessor);
		outputProcessor.unregisterInputStream(stream);

        correlatorRegisters.outputProcessors2ConsumerInstances.get(outputProcessor).clear();
	}


	@Override
	public EventStore getEventStore() {
		return eventStore;
	}


	@Override
	public void setEventStoreEnabled(boolean enabled) {
        eventStoreEnabled = enabled;
	}

    @Override
    public boolean isEventStoreEnabled() {
        return eventStoreEnabled;
    }


    @Override
	public void setEventStoreCapacity(int capacity, CapacityUnit capacityUnit) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Attribute[] getOutputSchema(EPA query) {
		return source2schema.get(query2stream.get(query.getName()));
	}


	@Override
	public void createRelation(String relation, Attribute[] schema) {
		//  Auto-generated method stub
		
	}


	@Override
	public void insertIntoRelation(String relation, Object[] tuple) {
		//  Auto-generated method stub
		
	}


	@Override
	public void deleteFromRelation(String relation, Object[] tuple) {
		//  Auto-generated method stub
		
	}
	
	
    /////////////////////////////////////////////////////////////////
	//                           HELPER                            //
	//            übernommen aus WebMethodsEngine.java             //
	/////////////////////////////////////////////////////////////////
	
	
    /**
     * Simulates the window operator on input streams. That is, it creates the physical stream of a raw stream.
     *
     * @param input the input event stream on which a virtual window operator is applied
     * @return the name of the output stream of the virtual window operator
     */
    private String createWindowedStream(Stream input, String query, long version) {
        String stream = "";
        Set<NativeEngineWindow> windows = source2windows.get(input.getName());
        if(windows == null)
            windows = new HashSet<>();
        WindowType type = WindowType.TIME_WINDOW;
        if(input.getWindow() instanceof CountWindow)
            type = WindowType.COUNT_WINDOW;
        stream = input.getName()+"$Window"+windowCount++;

        NativeEngineWindow queryWindow = new NativeEngineWindow(type, stream, input.getWindowSize());
        if(type == WindowType.TIME_WINDOW)
            queryWindow.initTimeWindow(((TimeWindow)(input.getWindow())).getJump());
        if(type == WindowType.COUNT_WINDOW)
            queryWindow.initCountWindow(((CountWindow)(input.getWindow())).getJump());
        if(type == WindowType.PARTITIONED_COUNT_WINDOW) {
            Attribute[] schema = source2schema.get(input.getName());
            String[] partitionBy = ((PartitionedCountWindow)input.getWindow()).getPartitionAttributes();
            int[] partitionAttributes = new int[partitionBy.length];
            int counter = 0;
            for(String partitionAttribute : partitionBy)
                for(int i = 0; i < schema.length; i++)
                    if(schema[i].getAttributeName().equals(partitionAttribute))
                        partitionAttributes[counter++] = i;
            queryWindow.initPartitionedCountWindow(((PartitionedCountWindow) input.getWindow()).getJump(), partitionAttributes);
        }
        windows.add(queryWindow);
        source2windows.put(input.getName(), windows);
        List<NativeEngineWindow> queryWindows = query2windows.get(query+"$"+version);
        if(queryWindows == null) {
            queryWindows = new ArrayList<>();
            query2windows.put(query+"$"+version, queryWindows);
        }
        queryWindows.add(queryWindow);
        return stream;
    }

    /**
     * Creates a new event stream derived from a relation.
     *
     * @param relationName the relation to derive from
     * @return the unique name of the derived event stream
     */
    private String createRelationStream(String relationName) {
        boolean eventStoreEnabledOld = eventStoreEnabled;
        eventStoreEnabled = false;
        registerStream(relationName+"$"+sharedRegisters.relationCount, source2schema.get(relationName));
        eventStoreEnabled = eventStoreEnabledOld;
        sharedRegisters.relationstreamToClock.put(relationName + "$" + sharedRegisters.relationCount, 0L);
        return relationName+"$"+sharedRegisters.relationCount++;
    }
	
	/**
	 * Computes the output schema of aggregator EPA.
	 * 
	 * @param d the aggregator EPA of which the output schema is computed
	 * @return the output schema of the aggregator EPA
	 */
	private Attribute[] getSchemaOfAggregator(Aggregator d) {
		Attribute[] schemaIn = source2schema.get(d.getInputEPA().getName());
		Aggregate[] aggregates = d.getAggregat();
        Attribute[] schema = new Attribute[aggregates.length+2];
		int i = 0; // attribute array pointer
		String attrName = "";
		DataType attrType = DataType.STRING;
		for(Aggregate agg : aggregates) {
			attrName = agg.getOutputAttribute();
			if(agg instanceof Group) {
				for(Attribute attr : schemaIn)
					if(attr.getAttributeName().equals(attrName))
						attrType = attr.getAttributeType();
			}
			else {
				if(agg instanceof Average || agg instanceof Stddev)
					attrType = DataType.DOUBLE;
				if(agg instanceof Count)
					attrType = DataType.LONG;
				if(agg instanceof Maximum || agg instanceof Minimum || agg instanceof Sum) {
					String aName = agg.getAttribute();
					for(Attribute attr : schemaIn)
						if(attr.getAttributeName().equals(aName))
							attrType = attr.getAttributeType();	
				}
			}
			schema[i++] = new Attribute(attrName, attrType);
		}
        schema[schema.length-2] = new Attribute("tstart", DataType.LONG);
        schema[schema.length-1] = new Attribute("tend", DataType.LONG);
		return schema;
	}


    /**
     * Gets the filter index for a given event stream.
     *
     * @param stream event stream the filter index is requested for
     * @return filter index if exists otherwise <code>null</code>
     */
    public FilterIndex getFilterIndex(String stream) {
        return source2FilterIndex.get(stream);
    }


    /**
     * Gets the BE tree for a given event stream.
     *
     * @param stream event stream the BE tree is requested for
     * @return BE tree if exists otherwise <code>null</code>
     */
    public BeTree getBeTree(String stream) {
        FilterIndex filterIndex = getFilterIndex(stream);
        if(filterIndex==null) {
            return null;
        }
        return filterIndex.getBeTree();
    }


    /**
     * When using bulkloading BE trees, then the index creation must be triggered
     * explicitly by users! IMPORTANT: Filter EPAs that are not indexed by a
     * BE tree are fully evaluated resulting in much higher costs!
     *
     * @param stream event stream the filter index is rebuild
     */
    public void bulkloadingRebuildBeTree(String stream) {
        if(filterIndexConfiguration.useFilterBulkLoading) {
            FilterIndex filterIndex = getFilterIndex(stream);
            if(filterIndex==null) {
                throw new RuntimeException();
            }
            filterIndex.bulkloadingRebuildBeTree();
        }
    }
	
	
    /////////////////////////////////////////////////////////////////
    //                     INNER CLASS WINDOW                      //
    /////////////////////////////////////////////////////////////////

	
    /**
     * Available window types.
     */
    public enum WindowType {

        /**
         * Time-based window.
         */
        TIME_WINDOW,

        PARTITIONED_COUNT_WINDOW, /**
         * Count-based window.
         */
        COUNT_WINDOW

    }
	
    /**
     * Represents windows.
     */
    class NativeEngineWindow {

        @Override
        public int hashCode() {
            return e.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return ((NativeEngineWindow)obj).e.equals(e);
        }

        /**
         * The type of this window.
         */
        public WindowType type;

        /**
         * The stream on which this window is applied.
         */
        public String e;

        /**
         * The size of this window.
         */
        public long SIZE;

        /**
         * The jump size of this window.
         */
        private long JUMP;

        private long DELAY;
        private long globalCounter;
        private long clock;  // clock of the window (used to set end timestamps of a slide)
        private long tstart; // start timestamp of a slide (this timestamp is determined by the last element of a slide)
        boolean isDelayed;
        private Object[] out;
        private List<Object[]> output;

        /**
         * Buffer needed by count-based windows to compute
         * end timestamps.
         */
        private ArrayDeque<Object[]> countBuffer;


        /**
         * Creates a new window.
         *
         * @param e the stream on which the new window is applied
         * @param size the size of the new window
         */
        public NativeEngineWindow(WindowType type, String e, long size) {
            this.type = type;
            this.e    = e;
            this.SIZE = size;
            if(type == WindowType.COUNT_WINDOW)
                countBuffer  = new ArrayDeque<>();
        }


        public void initTimeWindow(long jump) {
            this.JUMP = jump;
        }


        public void initCountWindow(long jump) {
            this.JUMP = jump;
            DELAY = JUMP;
            while(DELAY < SIZE) // slide until we reach size
                DELAY += JUMP;
            DELAY = DELAY-SIZE; // difference is number of events we have to delay
            clock         = -1;
            tstart        = -1;
            isDelayed     = false;
        }

        public void initPartitionedCountWindow(long jump, int[] partitionAttributes) {
            JUMP = jump;
            this.partitionAttributes = partitionAttributes;
            partitions = new HashMap<>();
            partitionedCountBuffer = new PriorityQueue<>(10, new Comparator<Object[]>() {
                @Override
                public int compare(Object[] o1, Object[] o2) {
                    return Long.compare((long) o1[o1.length-2], (long) o2[o2.length-2]);
                }
            });
        }


        /**
         * Pushes an event into this window.
         *
         * @param payload the payload of the event
         * @param t the start timestamp of the event
         * @return
         */
        public List<Object[]> pushCountBased(Object[] payload, long t) {
            output = null;
            Object[] event = new Object[payload.length+2];
            System.arraycopy(payload, 0, event, 0, payload.length);
            event[event.length-2] = t;
            if(t > clock) { // if time has progressed => Update clock and reset counter
                globalCounter = 0;
                clock         = t;
            }
            countBuffer.add(event);
            if(isDelayed && countBuffer.size() == SIZE + JUMP) { // if initial delay has already happened => perform slide by slide
                int counter  = 0;
                if(DELAY == 0)
                    tstart = (long)countBuffer.toArray(new Object[0][])[(int)JUMP-1][event.length-2];  // one non-delayed slide completed => update tstart
                output = new LinkedList<>();
                while(countBuffer.size() > SIZE) {
                    out = countBuffer.pollFirst();
                    if(globalCounter >= SIZE) // 'size' events reported in this slide => ignore the rest
                        continue;
                    if(counter < JUMP - DELAY) {
                        if(tstart != t) {
                            out[out.length-2] = tstart;
                            out[out.length-1] = t;
                            output.add(out);
                        }
                        counter++;
                        if(counter == JUMP - DELAY)  // one delayed SLIDE completed => update tstart
                            tstart = (long)countBuffer.toArray(new Object[0][])[(int)JUMP-1][event.length-2];
                    }
                    else if(tstart != t) {
                        out[out.length-2] = tstart;
                        out[out.length-1] = t;
                        output.add(out);
                    }
                    globalCounter++;
                }
            } else if(!isDelayed && countBuffer.size() == SIZE + DELAY) { // if initial delay has not happened yet, do it now
                tstart = (long)countBuffer.toArray(new Object[0][])[(int)JUMP-1][event.length-2];
                output = new LinkedList<>();
                while(countBuffer.size() > SIZE) {
                    out = countBuffer.pollFirst();
                    if(tstart != t) {
                        out[out.length-2] = tstart;
                        out[out.length-1] = t;
                        output.add(out);
                    }
                    globalCounter++;
                }
                isDelayed = true;
            }
            return output;
        }


        /**
         * Pushes an event into this window.
         *
         * @param payload the payload of the event
         * @param t the start timestamp of the event
         * @return
         */
        public Object[] pushTimeBased(Object[] payload, long t) {
            Object[] event = new Object[payload.length+2];
            System.arraycopy(payload, 0, event, 0, payload.length);
            event[event.length-2] = (t / JUMP) * JUMP;
            event[event.length-1] = ((t + SIZE) / JUMP) * JUMP;
            if(event[event.length-2] != event[event.length-1])
                return event;
            return null;
        }

        /**
         * Stores the positions of all attributes that are used
         * to partition the input event stream. Only needed by
         * partitioned count-based windows.
         */
        private int[] partitionAttributes;

        /**
         * Stores all active partitions. Only needed by partitioned
         * count-based windows.
         */
        private Map<String,NativeEngineWindow> partitions;

        /**
         * Buffers the output of partitioned count-based windows.
         * Only events with a timestamp that all active partitions
         * have reached are safe to be reported!
         */
        private PriorityQueue<Object[]> partitionedCountBuffer;

        public List<Object[]> pushPartitionedCountBased(Object[] payload, long t) {
            String hash = "";
            for(int i : partitionAttributes)
                hash += payload[i];
            NativeEngineWindow partition = partitions.get(hash);
            if(partition == null) {
                NativeEngineWindow newPartition = new NativeEngineWindow(WindowType.COUNT_WINDOW, e ,SIZE);
                newPartition.initCountWindow(JUMP);
                partitions.put(hash, newPartition);
                partition = newPartition;
            }
            List<Object[]> results = partition.pushCountBased(payload, t);
            if(results != null)
                partitionedCountBuffer.addAll(results);
            List<Object[]> output = new LinkedList<>();
            long minTimestamp = getMinTimestamp();         // TODO Splits wenn eine Partition hängt!!
            for(Iterator<Object[]> iter = partitionedCountBuffer.iterator(); iter.hasNext();) {
                Object[] tmp = iter.next();
                if((long) tmp[tmp.length-2] <= minTimestamp) {
                    output.add(tmp);
                    iter.remove();
                }
                else
                    break;
            }
            return output;
        }

        /**
         * Gets the minimum clock among all input streams.
         *
         * @return minimum clock among all input streams
         */
        private long getMinTimestamp() {
            long minTimestamp = Long.MAX_VALUE;
            for(NativeEngineWindow w : partitions.values())
                minTimestamp = Math.min(minTimestamp, w.tstart);
            return minTimestamp;
        }
    }

}

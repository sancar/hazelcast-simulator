/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.tests.map.domain.JsonSampleFactory;
import com.hazelcast.simulator.tests.map.domain.MetadataCreator;
import com.hazelcast.simulator.tests.map.domain.ObjectSampleFactory;
import com.hazelcast.simulator.tests.map.domain.SampleFactory;
import com.hazelcast.simulator.tests.map.domain.TweetJsonFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class PutGetPerfTest extends HazelcastTest {


    // properties
    public String strategy = QueryPerformanceTest.Strategy.PORTABLE.name();
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String mapname = "default";
    public Random random = new Random();

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<Integer, Object> map;

    private int[] keys;
    private Object[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(mapname);
        keys = generateIntKeys(itemCount, KeyLocality.SHARED, targetInstance);
    }

    @Prepare
    public void prepare() {
        throttlingLogger.info(strategy + " " + targetInstance.getConfig().getMapConfig(mapname).getInMemoryFormat() + " " + mapname + " " + useIndex + " " + itemCount);
        if (useIndex) {
            map.addIndex(IndexType.HASH, "stringVam");
        }

        Random random = new Random();
        values = new Object[itemCount];
        for (int i = 0; i < values.length; i++) {
            values[i] = createObject();
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep(prob = 1.0)
    public void put(ThreadState state) {
        map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public void set(ThreadState state) {
        map.set(state.randomWriteKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomWriteKey() {
            return keys[randomInt(keys.length)];
        }

        private Object randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    private Object createObject() {
        SampleFactory factory;
        MetadataCreator creator = new MetadataCreator();
        if (QueryPerformanceTest.Strategy.valueOf(strategy) == QueryPerformanceTest.Strategy.JSON) {
            factory = new JsonSampleFactory(new TweetJsonFactory(), creator);
        } else {
            DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(QueryPerformanceTest.Strategy.valueOf(strategy));
            factory = new ObjectSampleFactory(objectFactory, creator);
        }
        return factory.create();
    }
}

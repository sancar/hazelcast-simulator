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

import com.hazelcast.core.IMap;
import com.hazelcast.json.Json;
import com.hazelcast.json.JsonObject;
import com.hazelcast.json.JsonValue;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.map.domain.DomainObject;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class JsonPutGetTest extends HazelcastTest {


    // properties
    public String strategy = JsonTest.Strategy.PORTABLE.name();
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
            map.addIndex("stringVam", false);
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
        if (JsonTest.Strategy.valueOf(strategy) == JsonTest.Strategy.JSON) {
            return createJsonObject("key");
        } else {
            DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(JsonTest.Strategy.valueOf(strategy));
            return createNewDomainObject(objectFactory, "key");
        }
    }

    private DomainObject createNewDomainObject(DomainObjectFactory objectFactory, String key) {
        DomainObject o = objectFactory.newInstance();
        o.setKey(key);
        o.setStringVam(randomAlphanumeric(7));
        o.setDoubleVal(nextDouble(0.0, Double.MAX_VALUE));
        o.setLongVal(nextLong(0, 500));
        o.setIntVal(nextInt(0, Integer.MAX_VALUE));
        return o;
    }


    private JsonValue createJsonObject(String key) {
        JsonObject o = Json.object();
        o.set("key", key);
        o.set("stringVam", randomAlphanumeric(7));
        o.set("doubleVal", nextDouble(0.0, Double.MAX_VALUE));
        o.set("longVal", nextLong(0, 500));
        o.set("intVal", nextInt(0, Integer.MAX_VALUE));
        return o;
    }


}

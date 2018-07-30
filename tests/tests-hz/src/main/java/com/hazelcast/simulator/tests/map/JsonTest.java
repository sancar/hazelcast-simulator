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
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.DomainObject;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class JsonTest extends HazelcastTest {

    public enum Strategy {
        PORTABLE,
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        JSON
    }

    // properties
    public String strategy = Strategy.PORTABLE.name();
    public int itemCount = 100000;
    public boolean useIndex = false;
    public String mapname = "default";

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<String, Object> map;
    private Set<String> uniqueStrings;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(mapname);
        uniqueStrings = targetInstance.getSet(mapname);
    }

    @Prepare(global = true)
    public void prepare() {
        throttlingLogger.info(strategy + " " + targetInstance.getConfig().getMapConfig(mapname).getInMemoryFormat() + " " + mapname + " " + useIndex + " " + itemCount);
        if (useIndex) {
            map.addIndex("stringVam", false);
        }
        String[] strings = generateUniqueStrings(itemCount);

        Streamer<String, Object> streamer = StreamerFactory.getInstance(map);
        if (Strategy.valueOf(strategy) == Strategy.JSON) {
            for (String key : strings) {
                JsonValue o = createJsonObject(key);
                streamer.pushEntry(key, o);
            }
        } else {

            DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(Strategy.valueOf(strategy));
            for (String key : strings) {
                DomainObject o = createNewDomainObject(objectFactory, key);
                streamer.pushEntry(o.getKey(), o);
            }

        }
        streamer.await();
    }

    private String[] generateUniqueStrings(int uniqueStringsCount) {
        Set<String> stringsSet = new HashSet<String>(uniqueStringsCount);
        do {
            String randomString = RandomStringUtils.randomAlphabetic(30);
            stringsSet.add(randomString);
        } while (stringsSet.size() != uniqueStringsCount);
        uniqueStrings.addAll(stringsSet);
        return stringsSet.toArray(new String[uniqueStringsCount]);
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


    @BeforeRun
    public void beforeRun(BaseThreadState state) throws Exception {
//        state.localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
    }

    @TimeStep(prob = 1)
    public void getByStringIndex(BaseThreadState state) {
        Collection<Object> val = map.values(Predicates.equal("stringVam", "sancar"));
    }

}

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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.tests.map.domain.JsonSampleFactory;
import com.hazelcast.simulator.tests.map.domain.MetadataCreator;
import com.hazelcast.simulator.tests.map.domain.ObjectSampleFactory;
import com.hazelcast.simulator.tests.map.domain.SampleFactory;
import com.hazelcast.simulator.tests.map.domain.TweetJsonFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashSet;
import java.util.Set;


public class QueryPerformanceTest extends HazelcastTest {

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
    public String predicate = "createdAt=sancar";

    private String predicateLeft;
    private String predicateRight;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<Integer, Object> map;
    private Set<String> uniqueStrings;
    private SampleFactory factory;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(mapname);
        uniqueStrings = targetInstance.getSet(mapname);
        String[] pred = predicate.split("=");
        predicateLeft = pred[0];
        predicateRight = pred[1];

        MetadataCreator metadataCreator = new MetadataCreator();
        if (Strategy.valueOf(strategy) == Strategy.JSON) {
            factory = new JsonSampleFactory(new TweetJsonFactory(), metadataCreator);
        } else {
            DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(Strategy.valueOf(strategy));
            factory = new ObjectSampleFactory(objectFactory, metadataCreator);
        }
    }

    @Prepare(global = true)
    public void prepare() {
        throttlingLogger.info(strategy + " " + mapname + " " + useIndex + " " + itemCount + " " + predicateLeft + " " + predicateRight);
        if (useIndex) {
            throw new HazelcastException("use index setting not implemented");
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < itemCount; i++) {
            Object o = factory.create();
            streamer.pushEntry(i, o);
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

    @BeforeRun
    public void beforeRun(BaseThreadState state) throws Exception {
//        state.localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
    }

    @TimeStep(prob = 1)
    public Object getByStringIndex(BaseThreadState state) {
        return map.values(Predicates.equal(predicateLeft, predicateRight));
    }

    @TimeStep(prob = 0)
    public void put(BaseThreadState state) {
        map.put(state.randomInt(itemCount), factory.create());
    }

    @TimeStep(prob = 0)
    public Object get(BaseThreadState state) {
        return map.get(state.randomInt(itemCount));
    }
}

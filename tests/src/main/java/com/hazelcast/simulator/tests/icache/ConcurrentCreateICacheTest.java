/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.io.Serializable;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static junit.framework.Assert.assertEquals;

/**
 * In this test we concurrently call createCache. From multi clients/members we expect no exceptions
 * in the setup phase of this test. We count the number of {@link CacheException} thrown when creating the cache
 * from multi members and clients, and at verification we assert that no exceptions where thrown.
 */
public class ConcurrentCreateICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(ConcurrentCreateICacheTest.class);

    // properties
    public String baseName = ConcurrentCreateICacheTest.class.getSimpleName();

    private IList<Counter> counterList;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        counterList = hazelcastInstance.getList(baseName);

        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig config = new CacheConfig();
        config.setName(baseName);

        Counter counter = new Counter();
        try {
            cacheManager.createCache(baseName, config);
            counter.create++;
        } catch (CacheException e) {
            LOGGER.severe(baseName + ": createCache exception " + e, e);
            counter.createException++;
        }
        counterList.add(counter);
    }

    @Verify(global = true)
    public void verify() throws Exception {
        Counter total = new Counter();
        for (Counter counter : counterList) {
            total.add(counter);
        }
        LOGGER.info(baseName + ": " + total + " from " + counterList.size() + " worker threads");

        assertEquals(baseName + ": We expect 0 CacheException from multi node create cache calls", 0, total.createException);
    }

    @Run
    public void run() {
    }

    private static class Counter implements Serializable {

        public long create = 0;
        public long createException = 0;

        public void add(Counter counter) {
            create += counter.create;
            createException += counter.createException;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + " create=" + create
                    + ", createException=" + createException
                    + '}';
        }
    }
}

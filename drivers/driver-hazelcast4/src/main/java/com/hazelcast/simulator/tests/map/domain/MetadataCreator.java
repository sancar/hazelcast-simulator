package com.hazelcast.simulator.tests.map.domain;

import java.util.Date;
import java.util.Random;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class MetadataCreator {

    private static final String[] COUNTRIES = new String[] {
            "UK", "US", "TR", "CZ", "FR"
    };

    private static final String[] CITIES = new String[] {
            "Gotham", "Metropolis", "Duckburg", "District X", "Riverdale", "Smallville", "Star City",
            "Stepford", "Emerald City", "Derry", "Zion"
    };

    private Random random = new Random();

    public String getCreatedAt() {
        return new Date(nextLong(1 << 12, 1L << 41)).toString();
    }

    public String getIdStr() {
        return "" + random.nextInt();
    }

    public String getText() {
        return randomAlphanumeric(130);
    }

    public int getId() {
        return random.nextInt();
    }

    public String getName() {
        return randomAlphanumeric(random.nextInt(3) + 6);
    }

    public String getScreenName() {
        return randomAlphanumeric(8 + random.nextInt(2));
    }

    public String getCountry() {
        return COUNTRIES[random.nextInt(COUNTRIES.length)];
    }

    public String getCity() {
        return CITIES[random.nextInt(CITIES.length)];
    }

    public String getUrl() {
        return "www." + randomAlphanumeric(random.nextInt(5) + 3) + ".com";
    }

    public String getDescription() {
        return randomAlphanumeric(50);
    }
}

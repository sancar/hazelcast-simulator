package com.hazelcast.simulator.tests.map.domain;

public interface TweetUserObject {
    int getId();

    void setId(int id);

    String getName();

    void setName(String name);

    String getScreenName();

    void setScreenName(String screenName);

    TweetLocationObject getLocation();

    void setLocation(TweetLocationObject location);

    String getUrl();

    void setUrl(String url);

    String getDescription();

    void setDescription(String description);
}

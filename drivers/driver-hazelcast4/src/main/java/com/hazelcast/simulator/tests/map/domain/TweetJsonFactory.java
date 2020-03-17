package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;

public class TweetJsonFactory {

    protected String createdAt;
    protected String idStr;
    protected String text;

    // <user>
    protected int id;
    protected String name;
    protected String screenName;

    // <location>
    protected String country;
    protected String city;
    // </location>

    protected String url;
    protected String description;
    // </user>

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public void setIdStr(String idStr) {
        this.idStr = idStr;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String buildJsonText() {
        return build().toString();
    }

    public JsonValue build() {
        return Json.object()
                .add("createdAt", createdAt)
                .add("idStr", idStr)
                .add("text", text)
                .add("user", Json.object()
                        .add("id", id)
                        .add("name", name)
                        .add("screenName", screenName)
                        .add("location", Json.object()
                                .add("country", country)
                                .add("city", city))
                        .add("url", url)
                        .add("description", description));
    }
}

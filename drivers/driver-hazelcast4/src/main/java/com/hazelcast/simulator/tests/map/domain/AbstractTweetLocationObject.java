package com.hazelcast.simulator.tests.map.domain;

import java.io.Serializable;

public class AbstractTweetLocationObject implements TweetLocationObject, Serializable {

    protected String country;
    protected String city;

    @Override
    public String getCountry() {
        return country;
    }

    @Override
    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String getCity() {
        return city;
    }

    @Override
    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTweetLocationObject that = (AbstractTweetLocationObject) o;

        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        return city != null ? city.equals(that.city) : that.city == null;
    }

    @Override
    public int hashCode() {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + (city != null ? city.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AbstractTweetLocationObject{" +
                "country='" + country + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}

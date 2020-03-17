package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class IDSTweetLocationObject extends DSTweetLocationObject implements IdentifiedDataSerializable {

    static final int CLASS_ID = 3;

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableObjectFactory.FACTORY_ID;
    }
}

package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DSTweetLocationObject extends AbstractTweetLocationObject implements DataSerializable {

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeUTF(city);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        country = in.readUTF();
        city = in.readUTF();
    }
}

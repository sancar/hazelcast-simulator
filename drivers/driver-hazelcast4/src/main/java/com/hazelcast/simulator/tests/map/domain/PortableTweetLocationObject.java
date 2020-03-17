package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableTweetLocationObject extends AbstractTweetLocationObject implements Portable {

    static final int CLASS_ID = 3;

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return PortableObjectFactory.FACTORY_ID;
    }


    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("country", country);
        writer.writeUTF("city", city);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        country = reader.readUTF("country");
        city = reader.readUTF("city");
    }
}

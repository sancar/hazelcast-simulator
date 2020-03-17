package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableTweetUserObject extends AbstractTweetUserObject implements Portable {

    static final int CLASS_ID = 2;


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
        writer.writeInt("id", id);
        writer.writeUTF("name", name);
        writer.writeUTF("screenName", screenName);
        writer.writePortable("location", (PortableTweetLocationObject)location);
        writer.writeUTF("url", url);
        writer.writeUTF("description", description);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        id = reader.readInt("id");
        name = reader.readUTF("name");
        screenName = reader.readUTF("screenName");
        location = reader.readPortable("location");
        url = reader.readPortable("url");
        description = reader.readUTF("description");
    }
}

package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DSTweetUserObject extends AbstractTweetUserObject implements DataSerializable {
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeUTF(screenName);
        out.writeObject(location);
        out.writeUTF(url);
        out.writeUTF(description);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
        screenName = in.readUTF();
        location = in.readObject();
        url = in.readUTF();
        description = in.readUTF();
    }
}

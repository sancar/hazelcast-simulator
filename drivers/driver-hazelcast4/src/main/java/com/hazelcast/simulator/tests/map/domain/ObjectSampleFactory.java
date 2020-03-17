package com.hazelcast.simulator.tests.map.domain;

public class ObjectSampleFactory implements SampleFactory {

    private DomainObjectFactory factory;
    private MetadataCreator creator;

    public ObjectSampleFactory(DomainObjectFactory factory, MetadataCreator creator) {
        this.factory = factory;
        this.creator = creator;
    }

    @Override
    public TweetObject create() {
        TweetLocationObject locationObject = factory.newLocationObject();
        locationObject.setCity(creator.getCity());
        locationObject.setCountry(creator.getCountry());

        TweetUserObject userObject = factory.newUserObject();
        userObject.setDescription(creator.getDescription());
        userObject.setId(creator.getId());
        userObject.setLocation(locationObject);
        userObject.setName(creator.getName());
        userObject.setScreenName(creator.getScreenName());
        userObject.setUrl(creator.getUrl());

        TweetObject object = factory.newObject();
        object.setCreatedAt(creator.getCreatedAt());
        object.setIdStr(creator.getIdStr());
        object.setText(creator.getText());
        object.setUser(userObject);

        return object;
    }
}

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.domain;

import java.io.Serializable;

abstract class AbstractTweetObject implements TweetObject, Serializable {

    protected String createdAt;
    protected String idStr;
    protected String text;
    protected TweetUserObject user;

    @Override
    public String getCreatedAt() {
        return createdAt;
    }

    @Override
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String getIdStr() {
        return idStr;
    }

    @Override
    public void setIdStr(String idStr) {
        this.idStr = idStr;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public void setText(String text) {
        this.text = text;
    }

    @Override
    public TweetUserObject getUser() {
        return user;
    }

    @Override
    public void setUser(TweetUserObject user) {
        this.user = user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTweetObject that = (AbstractTweetObject) o;

        if (createdAt != null ? !createdAt.equals(that.createdAt) : that.createdAt != null) return false;
        if (idStr != null ? !idStr.equals(that.idStr) : that.idStr != null) return false;
        if (text != null ? !text.equals(that.text) : that.text != null) return false;
        return user != null ? user.equals(that.user) : that.user == null;
    }

    @Override
    public int hashCode() {
        int result = createdAt != null ? createdAt.hashCode() : 0;
        result = 31 * result + (idStr != null ? idStr.hashCode() : 0);
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AbstractTweetObject{" +
                "createdAt='" + createdAt + '\'' +
                ", idStr='" + idStr + '\'' +
                ", text='" + text + '\'' +
                ", user=" + user +
                '}';
    }
}

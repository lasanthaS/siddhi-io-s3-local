/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.s3.sink.internal.beans;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class EventObject {
    protected SinkConfig config;
    protected String objectPath;
    protected List<Object> events;

    public EventObject(SinkConfig config, String objectPath) {
        this.config = config;
        this.objectPath = objectPath;
        this.events = new ArrayList<>();
    }

    public SinkConfig getConfig() {
        return config;
    }

    public void setConfig(SinkConfig config) {
        this.config = config;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    public List<Object> getEvents() {
        return events;
    }

    public void setEvents(List<Object> events) {
        this.events = events;
    }

    public void addEvent(Object event) {
        this.events.add(event);
    }

    public int getEventCount() {
        return this.events.size();
    }

    @Override
    public String toString() {
        return String.format("Event[path=%s, count=%s]", this.objectPath, this.events.size());
    }

    public abstract InputStream serialize();

    public abstract String getObjectKey();
}
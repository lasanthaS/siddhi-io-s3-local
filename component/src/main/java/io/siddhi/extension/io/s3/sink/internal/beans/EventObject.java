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

import java.util.ArrayList;
import java.util.List;

public class EventObject {
    private String objectPath;
    private List<Object> events;
    private int offset;
    private String bucketName;

    public EventObject(String bucketName, String objectPath, int offset) {
        this.bucketName = bucketName;
        this.objectPath = objectPath;
        this.offset = offset;
        this.events = new ArrayList<>();

        System.out.println(">>>>>>>>>>> EventObject created with offset: " + offset);
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
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

    public String serialize() {
        return this.toString();
    }

    @Override
    public String toString() {
        return String.format("Event[offset=%s, path=%s, count=%s]", this.offset, this.objectPath, this.events.size());
    }
}
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

import com.amazonaws.regions.Regions;

public class SinkConfig {
    private String storageClass = "standard";
    private String credentialProviderClass = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    private String bucketName = null;
    private String awsRegion = Regions.DEFAULT_REGION.getName();
    private boolean versioningEnabled;
    private int flushSize = -1;
    private int rotateIntetrvalMs = -1;
    private int rotateScheduledIntervalMs = -1;

    public SinkConfig() {
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public String getCredentialProviderClass() {
        return credentialProviderClass;
    }

    public void setCredentialProviderClass(String credentialProviderClass) {
        this.credentialProviderClass = credentialProviderClass;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public void setVersioningEnabled(boolean versioningEnabled) {
        this.versioningEnabled = versioningEnabled;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public void setFlushSize(int flushSize) {
        this.flushSize = flushSize;
    }

    public int getRotateIntetrvalMs() {
        return rotateIntetrvalMs;
    }

    public void setRotateIntetrvalMs(int rotateIntetrvalMs) {
        this.rotateIntetrvalMs = rotateIntetrvalMs;
    }

    public int getRotateScheduledIntervalMs() {
        return rotateScheduledIntervalMs;
    }

    public void setRotateScheduledIntervalMs(int rotateScheduledIntervalMs) {
        this.rotateScheduledIntervalMs = rotateScheduledIntervalMs;
    }
}

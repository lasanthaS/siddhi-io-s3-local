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

package io.siddhi.extension.io.s3.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.publisher.EventPublisher;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.utils.ServiceClient;
import io.siddhi.query.api.definition.StreamDefinition;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

/**
 bucket.acl
 object.acl(check httpheaders)
 object.metadata
 */

@Extension(
        name = "s3",
        namespace = "sink",
        description = " ",
        parameters = {
                @Parameter(
                        name = "credential.provider.class",
                        type = DataType.STRING,
                        description = "AWS credential provider class to be used. If blank along with the username " +
                                "and the password, default credential provider will be used.",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "aws.access.key",
                        type = DataType.STRING,
                        description = "AWS access key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "aws.secret.key",
                        type = DataType.STRING,
                        description = "AWS secret key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "bucket.name",
                        type = DataType.STRING,
                        description = "Name of the S3 bucket"
                ),
                @Parameter(
                        name = "aws.region",
                        type = DataType.STRING,
                        description = "The region to be used to create the bucket",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "versioning.enabled",
                        type = DataType.BOOL,
                        description = "Flag to enable versioning support in the bucket",
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "object.path",
                        type = DataType.STRING,
                        description = "Path for each S3 object",
                        dynamic = true
                ),
                @Parameter(
                        name = "storage.class",
                        type = DataType.STRING,
                        description = "AWS storage class",
                        optional = true,
                        defaultValue = "standard"
                ),
                @Parameter(
                        name = "flush.size",
                        type = DataType.INT,
                        description = "Maximum number of events to be written into a file",
                        optional = true,
                        defaultValue = "1"
                ),
                @Parameter(
                        name = "rotate.interval.ms",
                        type = DataType.INT,
                        description = "Maximum span of event time",
                        optional = true,
                        defaultValue = "-1"
                ),
                @Parameter(
                        name = "rotate.scheduled.interval.ms",
                        type = DataType.INT,
                        description = "Maximum span of event time from the first event",
                        optional = true,
                        defaultValue = "-1"
                ),
                @Parameter(
                        name = "content.type",
                        type = DataType.STRING,
                        description = "Content type of the event",
                        optional = true,
                        defaultValue = "application/octet-stream",
                        dynamic = true
                )
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)
// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/#sink
public class S3Sink extends Sink {

    private SinkConfig config;
    private String mapType;
    private EventPublisher publisher;
    private OptionHolder optionHolder;

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     *
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Event.class};
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{S3Constants.OBJECT_PATH};
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param streamDefinition containing stream definition bind to the {@link Sink}
     * @param optionHolder     Option holder containing static and dynamic configuration related
     *                         to the {@link Sink}
     * @param configReader     to read the sink related system configuration.
     * @param siddhiAppContext the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     *                         get siddhi related utility functions.
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                                SiddhiAppContext siddhiAppContext) {
        this.optionHolder = optionHolder;
        this.config = this.buildConfig(optionHolder);

        if (streamDefinition.getAnnotations().get(0).getAnnotations().size() > 0) {
            // todo better to get this using keys instead of the indices.
            this.mapType = streamDefinition.getAnnotations().get(0).getAnnotations().get(0).getElements().get(0).getValue();
        }
        return null;
    }

    /**
     * This method will be called when events need to be published via this sink
     *
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @param state          current state of the sink
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        this.publisher.publish(payload, dynamicOptions);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        this.publisher = new EventPublisher(this.config, this.optionHolder);
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {
        // todo publish all queued events before shutdown
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {
        // Not applicable
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    private SinkConfig buildConfig(OptionHolder optionHolder) {
        SinkConfig config = new SinkConfig();
        optionHolder.getStaticOptionsKeys().forEach(key -> {
            switch (key) {
                case S3Constants.CREDENTIAL_PROVIDER_CLASS:
                    config.setCredentialProviderClass(
                            optionHolder.validateAndGetStaticValue(S3Constants.CREDENTIAL_PROVIDER_CLASS));
                    break;
                case S3Constants.AWS_ACCESS_KEY:
                    config.setAwsAccessKey(optionHolder.validateAndGetStaticValue(S3Constants.AWS_ACCESS_KEY));
                    break;
                case S3Constants.AWS_SECRET_KEY:
                    config.setAwsSecretKey(optionHolder.validateAndGetStaticValue(S3Constants.AWS_SECRET_KEY));
                    break;
                case S3Constants.AWS_REGION:
                    config.setAwsRegion(optionHolder.validateAndGetStaticValue(S3Constants.AWS_REGION));
                    break;
                case S3Constants.BUCKET_NAME:
                    config.setBucketName(optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_NAME));
                    break;
                case S3Constants.VERSIONING_ENABLED:
                    config.setVersioningEnabled(Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(S3Constants.VERSIONING_ENABLED)));
                    break;
                case S3Constants.STORAGE_CLASS:
                    config.setStorageClass(optionHolder.validateAndGetStaticValue(S3Constants.STORAGE_CLASS));
                    break;
                case S3Constants.FLUSH_SIZE:
                    config.setFlushSize(Integer.parseInt(
                            optionHolder.validateAndGetStaticValue(S3Constants.FLUSH_SIZE)));
                    break;
                case S3Constants.ROTATE_INTERVAL_MS:
                    config.setRotateIntetrvalMs(Integer.parseInt(
                            optionHolder.validateAndGetStaticValue(S3Constants.ROTATE_INTERVAL_MS)));
                    break;
                case S3Constants.ROTATE_SCHEDULED_INTERVAL_MS:
                    config.setRotateScheduledIntervalMs(Integer.parseInt(
                            optionHolder.validateAndGetStaticValue(S3Constants.ROTATE_SCHEDULED_INTERVAL_MS)));
                    break;
                default:
                    // Not an option
            }
        });

        if (config.getBucketName() == null || config.getBucketName().isEmpty()) {
            throw new IllegalArgumentException("Parameter 'bucket.name' is required");
        }

        if (!optionHolder.isOptionExists(S3Constants.OBJECT_PATH)) {
            throw new IllegalArgumentException("Parameter 'object.path' is required");
        }
        return config;
    }
}

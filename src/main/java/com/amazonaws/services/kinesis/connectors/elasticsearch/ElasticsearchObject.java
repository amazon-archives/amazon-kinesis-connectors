/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.connectors.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * ElasticsearchObject defines the information needed by the ElasticsearchEmitter
 * to properly index the record. It is to be instantiated by the ElasticsearchTransformer
 * as the final output. Information should be passed in via the constructor
 * during the fromClass transformation.
 */
public class ElasticsearchObject {

    /**
     * The index name within Elasticsearch to store the source document.
     */
    private String index;

    /**
     * The type of the source to store within the index.
     */
    private String type;

    /**
     * The JSON document to store in Elasticsearch.
     */
    private String source;

    /**
     * The id to store the object under.
     * If null, elasticsearch will automatically generate one.
     */
    private String id;

    /**
     * The version number to support optimistic locking when indexing objects.
     * If null, no optimistic locking will be used.
     */
    private Long version;

    /**
     * The time to live (ttl) for the object. Note that the default ttl
     * mapping must be set to the relevant index for this to work.
     * If null, no time to live will be set for object.
     */
    private Long ttl;

    /**
     * Instruct the index request to only index the object if it does not
     * already exist.
     * If null, no create flag will be set.
     */
    private Boolean create;

    public ElasticsearchObject(String index, String type, String source) {
        this(index, type, null, source);
    }

    public ElasticsearchObject(String index, String type, String id, String source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
        this.version = null;
        this.ttl = null;
        this.create = null;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getSource() {
        return source;
    }

    public String getId() {
        return id;
    }

    public Long getVersion() {
        return version;
    }

    public Long getTtl() {
        return ttl;
    }

    public Boolean getCreate() {
        return create;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public void setCreate(boolean create) {
        this.create = create;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.janusgraph.kggraph.json;

import org.apache.tinkerpop.gremlin.structure.io.graphson.ToStringGraphSONSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.DefaultSerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.ser.SerializerFactory;

/**
 * Implementation of the {@code DefaultSerializerProvider} for Jackson that uses the {@code ToStringSerializer} for
 * unknown types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class TrsGraphSONSerializerProvider extends DefaultSerializerProvider {
    private static final long serialVersionUID = 1L;
    private final JsonSerializer<Object> unknownTypeSerializer;

    public TrsGraphSONSerializerProvider(final TrsGraphSONVersion version) {
        super();
        unknownTypeSerializer = new ToStringGraphSONSerializer();
    }

    protected TrsGraphSONSerializerProvider(final SerializerProvider src,
                                            final SerializationConfig config, final SerializerFactory f,
                                            final JsonSerializer<Object> unknownTypeSerializer) {
        super(src, config, f);
        this.unknownTypeSerializer = unknownTypeSerializer;
    }

    @Override
    public JsonSerializer<Object> getUnknownTypeSerializer(final Class<?> aClass) {
        return unknownTypeSerializer;
    }

    @Override
    public TrsGraphSONSerializerProvider createInstance(final SerializationConfig config,
                                                        final SerializerFactory jsf) {
        // createInstance is called pretty often to create a new SerializerProvider
        // we give it the unknownTypeSerializer that we had in the first place,
        // when the object was first constructed through the public constructor
        // that has a TrsGraphSONVersion.
        return new TrsGraphSONSerializerProvider(this, config, jsf, unknownTypeSerializer);
    }
}

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

import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationConfig;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.NamedType;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.PolymorphicTypeValidator;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import java.util.Collection;

/**
 * Creates the Type serializers as well as the Typed deserializers that will be provided to the serializers and
 * deserializers. Contains the typeInfo level that should be provided by the GraphSONMapper.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TrsGraphSONTypeResolverBuilder extends StdTypeResolverBuilder {

    private TypeInfo typeInfo;
    private String valuePropertyName;
    private final TrsGraphSONVersion version;
    private final PolymorphicTypeValidator typeValidator = BasicPolymorphicTypeValidator.builder().build();

    public TrsGraphSONTypeResolverBuilder(final TrsGraphSONVersion version) {
        this.version = version;
    }

    @Override
    public TypeDeserializer buildTypeDeserializer(final DeserializationConfig config, final JavaType baseType,
                                                  final Collection<NamedType> subtypes) {
        final TypeIdResolver idRes = this.idResolver(config, baseType, typeValidator, subtypes, false, true);
        return new TrsGraphSONTypeDeserializer(baseType, idRes, this.getTypeProperty(), typeInfo, valuePropertyName);
    }


    @Override
    public TypeSerializer buildTypeSerializer(final SerializationConfig config, final JavaType baseType,
                                              final Collection<NamedType> subtypes) {
        final TypeIdResolver idRes = this.idResolver(config, baseType, typeValidator, subtypes, true, false);
        return new TrsGraphSONTypeSerializer(idRes, this.getTypeProperty(), typeInfo, valuePropertyName);
    }

    public TrsGraphSONTypeResolverBuilder valuePropertyName(final String valuePropertyName) {
        this.valuePropertyName = valuePropertyName;
        return this;
    }

    public TrsGraphSONTypeResolverBuilder typesEmbedding(final TypeInfo typeInfo) {
        this.typeInfo = typeInfo;
        return this;
    }
}

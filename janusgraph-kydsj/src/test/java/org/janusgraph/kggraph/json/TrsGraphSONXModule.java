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

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Version 3.0 of GraphSON extensions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TrsGraphSONXModule extends TrsGraphSONModule {

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(ByteBuffer.class, "ByteBuffer");
                put(Short.class, "Int16");
                put(BigInteger.class, "BigInteger");
                put(BigDecimal.class, "BigDecimal");
                put(Byte.class, "Byte");
                put(Character.class, "Char");
                put(InetAddress.class, "InetAddress");

                // Time serializers/deserializers
                put(Duration.class, "Duration");
                put(Instant.class, "Instant");
                put(LocalDate.class, "LocalDate");
                put(LocalDateTime.class, "LocalDateTime");
                put(LocalTime.class, "LocalTime");
                put(MonthDay.class, "MonthDay");
                put(OffsetDateTime.class, "OffsetDateTime");
                put(OffsetTime.class, "OffsetTime");
                put(Period.class, "Period");
                put(Year.class, "Year");
                put(YearMonth.class, "YearMonth");
                put(ZonedDateTime.class, "ZonedDateTime");
                put(ZoneOffset.class, "ZoneOffset");
            }});

    /**
     * Constructs a new object.
     */
    protected TrsGraphSONXModule(final boolean normalize,final boolean includePropertyProperty) {
        super("graphsonx-3.0");

        /////////////////////// SERIALIZERS ////////////////////////////

        // java.time
        addSerializer(Duration.class, new TrsJavaTimeSerializers.DurationJacksonSerializer());
        addSerializer(Instant.class, new TrsJavaTimeSerializers.InstantJacksonSerializer());
        addSerializer(LocalDate.class, new TrsJavaTimeSerializers.LocalDateJacksonSerializer());
        addSerializer(LocalDateTime.class, new TrsJavaTimeSerializers.LocalDateTimeJacksonSerializer());
        addSerializer(LocalTime.class, new TrsJavaTimeSerializers.LocalTimeJacksonSerializer());
        addSerializer(MonthDay.class, new TrsJavaTimeSerializers.MonthDayJacksonSerializer());
        addSerializer(OffsetDateTime.class, new TrsJavaTimeSerializers.OffsetDateTimeJacksonSerializer());
        addSerializer(OffsetTime.class, new TrsJavaTimeSerializers.OffsetTimeJacksonSerializer());
        addSerializer(Period.class, new TrsJavaTimeSerializers.PeriodJacksonSerializer());
        addSerializer(Year.class, new TrsJavaTimeSerializers.YearJacksonSerializer());
        addSerializer(YearMonth.class, new TrsJavaTimeSerializers.YearMonthJacksonSerializer());
        addSerializer(ZonedDateTime.class, new TrsJavaTimeSerializers.ZonedDateTimeJacksonSerializer());
        addSerializer(ZoneOffset.class, new TrsJavaTimeSerializers.ZoneOffsetJacksonSerializer());

        /////////////////////// DESERIALIZERS ////////////////////////////

        // java.time
        addDeserializer(Duration.class, new TrsJavaTimeSerializers.DurationJacksonDeserializer());
        addDeserializer(Instant.class, new TrsJavaTimeSerializers.InstantJacksonDeserializer());
        addDeserializer(LocalDate.class, new TrsJavaTimeSerializers.LocalDateJacksonDeserializer());
        addDeserializer(LocalDateTime.class, new TrsJavaTimeSerializers.LocalDateTimeJacksonDeserializer());
        addDeserializer(LocalTime.class, new TrsJavaTimeSerializers.LocalTimeJacksonDeserializer());
        addDeserializer(MonthDay.class, new TrsJavaTimeSerializers.MonthDayJacksonDeserializer());
        addDeserializer(OffsetDateTime.class, new TrsJavaTimeSerializers.OffsetDateTimeJacksonDeserializer());
        addDeserializer(OffsetTime.class, new TrsJavaTimeSerializers.OffsetTimeJacksonDeserializer());
        addDeserializer(Period.class, new TrsJavaTimeSerializers.PeriodJacksonDeserializer());
        addDeserializer(Year.class, new TrsJavaTimeSerializers.YearJacksonDeserializer());
        addDeserializer(YearMonth.class, new TrsJavaTimeSerializers.YearMonthJacksonDeserializer());
        addDeserializer(ZonedDateTime.class, new TrsJavaTimeSerializers.ZonedDateTimeJacksonDeserializer());
        addDeserializer(ZoneOffset.class, new TrsJavaTimeSerializers.ZoneOffsetJacksonDeserializer());
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return GraphSONTokens.GREMLINX_TYPE_NAMESPACE;
    }

    public static final class Builder implements GraphSONModuleBuilder {

        private Builder() {
        }

        @Override
        public TrsGraphSONModule create(final boolean normalize,final boolean includePropertyProperty) {
            return new TrsGraphSONXModule(normalize,includePropertyProperty);
        }
    }
}

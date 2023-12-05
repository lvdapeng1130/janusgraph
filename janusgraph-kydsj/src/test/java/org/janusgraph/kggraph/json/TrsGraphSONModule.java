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

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ByModulatorOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.gremlin.structure.util.star.DirectionalStarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV3d0;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The set of serializers that handle the core graph interfaces.  These serializers support normalization which
 * ensures that generated GraphSON will be compatible with line-based versioning tools. This setting comes with
 * some overhead, with respect to key sorting and other in-memory operations.
 * <p/>
 * This is a base class for grouping these core serializers.  Concrete extensions of this class represent a "version"
 * that should be registered with the {@link GraphSONVersion} enum.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TrsGraphSONModule extends TinkerPopJacksonModule {

    TrsGraphSONModule(final String name) {
        super(name);
    }

    /**
     * Attempt to load {@code SparqlStrategy} if it's on the path. Dynamically loading it from core makes it easier
     * for users as they won't have to register special modules for serialization purposes.
     */
    private static Optional<Class<?>> tryLoadSparqlStrategy() {
        try {
            return Optional.of(Class.forName("org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy"));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    /**
     * Version 3.0 of GraphSON.
     */
    static final class GraphSONModuleV3d0 extends TrsGraphSONModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    // Those don't have deserializers because handled by Jackson,
                    // but we still want to rename them in GraphSON
                    put(Integer.class, "Int32");
                    put(Long.class, "Int64");
                    put(Double.class, "Double");
                    put(Float.class, "Float");

                    put(Map.class, "Map");
                    put(List.class, "List");
                    put(Set.class, "Set");

                    // TinkerPop Graph objects
                    put(Lambda.class, "Lambda");
                    put(Vertex.class, "Vertex");
                    put(Edge.class, "Edge");
                    put(Property.class, "Property");
                    put(Path.class, "Path");
                    put(VertexProperty.class, "VertexProperty");
                    put(Metrics.class, "Metrics");
                    put(TraversalMetrics.class, "TraversalMetrics");
                    put(TraversalExplanation.class, "TraversalExplanation");
                    put(Traverser.class, "Traverser");
                    put(Tree.class, "Tree");
                    put(BulkSet.class, "BulkSet");
                    put(Bytecode.class, "Bytecode");
                    put(Bytecode.Binding.class, "Binding");
                    put(AndP.class, "P");
                    put(OrP.class, "P");
                    put(P.class, "P");
                    put(TextP.class, "TextP");
                    Stream.of(
                            VertexProperty.Cardinality.class,
                            Column.class,
                            Direction.class,
                            Operator.class,
                            Order.class,
                            Pop.class,
                            SackFunctions.Barrier.class,
                            TraversalOptionParent.Pick.class,
                            Scope.class,
                            T.class).forEach(e -> put(e, e.getSimpleName()));
                    Arrays.asList(
                            ConnectiveStrategy.class,
                            ElementIdStrategy.class,
                            EventStrategy.class,
                            HaltedTraverserStrategy.class,
                            PartitionStrategy.class,
                            SubgraphStrategy.class,
                            SeedStrategy.class,
                            LazyBarrierStrategy.class,
                            MatchAlgorithmStrategy.class,
                            AdjacentToIncidentStrategy.class,
                            ByModulatorOptimizationStrategy.class,
                            CountStrategy.class,
                            FilterRankingStrategy.class,
                            IdentityRemovalStrategy.class,
                            IncidentToAdjacentStrategy.class,
                            InlineFilterStrategy.class,
                            MatchPredicateStrategy.class,
                            OrderLimitStrategy.class,
                            OptionsStrategy.class,
                            PathProcessorStrategy.class,
                            PathRetractionStrategy.class,
                            RepeatUnrollStrategy.class,
                            ComputerVerificationStrategy.class,
                            LambdaRestrictionStrategy.class,
                            ReadOnlyStrategy.class,
                            StandardVerificationStrategy.class,
                            EarlyLimitStrategy.class,
                            EdgeLabelVerificationStrategy.class,
                            ReservedKeysVerificationStrategy.class,
                            //
                            GraphFilterStrategy.class,
                            VertexProgramStrategy.class
                    ).forEach(strategy -> put(strategy, strategy.getSimpleName()));

                    TrsGraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> put(s, s.getSimpleName()));
                }});

        /**
         * Constructs a new object.
         */
        public GraphSONModuleV3d0(final boolean normalize,final boolean includePropertyProperty) {
            super("graphson-3.0");

            /////////////////////// SERIALIZERS ////////////////////////////

            // graph
            addSerializer(Edge.class, new TrsGraphSONSerializers.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new TrsGraphSONSerializers.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new TrsGraphSONSerializers.VertexPropertyJacksonSerializer(normalize, true,includePropertyProperty));
            addSerializer(Property.class, new TrsGraphSONSerializers.PropertyJacksonSerializer());
            addSerializer(Metrics.class, new TrsGraphSONSerializers.MetricsJacksonSerializer());
            addSerializer(TraversalMetrics.class, new TrsGraphSONSerializers.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new TrsGraphSONSerializers.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new TrsGraphSONSerializers.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV3d0(normalize));
            addSerializer(Tree.class, new TrsGraphSONSerializers.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new TrsJavaUtilSerializers.MapEntryJacksonSerializer());
            addSerializer(Map.class, new TrsJavaUtilSerializers.MapJacksonSerializer());
            addSerializer(List.class, new TrsJavaUtilSerializers.ListJacksonSerializer());
            addSerializer(Set.class, new TrsJavaUtilSerializers.SetJacksonSerializer());

            // need to explicitly add serializers for those types because Jackson doesn't do it at all.
            addSerializer(Integer.class, new TrsGraphSONSerializers.IntegerGraphSONSerializer());
            addSerializer(Double.class, new TrsGraphSONSerializers.DoubleGraphSONSerializer());

            // traversal
            addSerializer(BulkSet.class, new TrsTraversalSerializers.BulkSetJacksonSerializer());
            addSerializer(Traversal.class, new TrsTraversalSerializers.TraversalJacksonSerializer());
            addSerializer(Bytecode.class, new TrsTraversalSerializers.BytecodeJacksonSerializer());
            Stream.of(VertexProperty.Cardinality.class,
                    Column.class,
                    Direction.class,
                    Operator.class,
                    Order.class,
                    Pop.class,
                    SackFunctions.Barrier.class,
                    Scope.class,
                    TraversalOptionParent.Pick.class,
                    T.class).forEach(e -> addSerializer(e, new TrsTraversalSerializers.EnumJacksonSerializer()));
            addSerializer(P.class, new TrsTraversalSerializers.PJacksonSerializer());
            addSerializer(Lambda.class, new TrsTraversalSerializers.LambdaJacksonSerializer());
            addSerializer(Bytecode.Binding.class, new TrsTraversalSerializers.BindingJacksonSerializer());
            addSerializer(Traverser.class, new TrsTraversalSerializers.TraverserJacksonSerializer());
            addSerializer(TraversalStrategy.class, new TrsTraversalSerializers.TraversalStrategyJacksonSerializer());

            /////////////////////// DESERIALIZERS ////////////////////////////

            // Tinkerpop Graph
            addDeserializer(Vertex.class, new TrsGraphSONSerializers.VertexJacksonDeserializer());
            addDeserializer(Edge.class, new TrsGraphSONSerializers.EdgeJacksonDeserializer());
            addDeserializer(Property.class, new TrsGraphSONSerializers.PropertyJacksonDeserializer());
            addDeserializer(Path.class, new TrsGraphSONSerializers.PathJacksonDeserializer());
            addDeserializer(TraversalExplanation.class, new TrsGraphSONSerializers.TraversalExplanationJacksonDeserializer());
            addDeserializer(VertexProperty.class, new TrsGraphSONSerializers.VertexPropertyJacksonDeserializer());
            addDeserializer(Metrics.class, new TrsGraphSONSerializers.MetricsJacksonDeserializer());
            addDeserializer(TraversalMetrics.class, new TrsGraphSONSerializers.TraversalMetricsJacksonDeserializer());
            addDeserializer(Tree.class, new TrsGraphSONSerializers.TreeJacksonDeserializer());

            // java.util
            addDeserializer(Map.class, new TrsJavaUtilSerializers.MapJacksonDeserializer());
            addDeserializer(List.class, new TrsJavaUtilSerializers.ListJacksonDeserializer());
            addDeserializer(Set.class, new TrsJavaUtilSerializers.SetJacksonDeserializer());

            // numbers
            addDeserializer(Integer.class, new TrsGraphSONSerializers.IntegerJackonsDeserializer());
            addDeserializer(Double.class, new TrsGraphSONSerializers.DoubleJacksonDeserializer());

            // traversal
            addDeserializer(BulkSet.class, new TrsTraversalSerializers.BulkSetJacksonDeserializer());
            addDeserializer(Bytecode.class, new TrsTraversalSerializers.BytecodeJacksonDeserializer());
            addDeserializer(Bytecode.Binding.class, new TrsTraversalSerializers.BindingJacksonDeserializer());
            Stream.of(VertexProperty.Cardinality.values(),
                    Column.values(),
                    Direction.values(),
                    Operator.values(),
                    Order.values(),
                    Pop.values(),
                    SackFunctions.Barrier.values(),
                    Scope.values(),
                    TraversalOptionParent.Pick.values(),
                    T.values()).flatMap(Stream::of).forEach(e -> addDeserializer(e.getClass(), new TrsTraversalSerializers.EnumJacksonDeserializer(e.getDeclaringClass())));
            addDeserializer(P.class, new TrsTraversalSerializers.PJacksonDeserializer());
            addDeserializer(TextP.class, new TrsTraversalSerializers.TextPJacksonDeserializer());
            addDeserializer(Lambda.class, new TrsTraversalSerializers.LambdaJacksonDeserializer());
            addDeserializer(Traverser.class, new TrsTraversalSerializers.TraverserJacksonDeserializer());
            Arrays.asList(
                    ConnectiveStrategy.class,
                    ElementIdStrategy.class,
                    EventStrategy.class,
                    HaltedTraverserStrategy.class,
                    PartitionStrategy.class,
                    SubgraphStrategy.class,
                    SeedStrategy.class,
                    LazyBarrierStrategy.class,
                    MatchAlgorithmStrategy.class,
                    AdjacentToIncidentStrategy.class,
                    ByModulatorOptimizationStrategy.class,
                    CountStrategy.class,
                    FilterRankingStrategy.class,
                    IdentityRemovalStrategy.class,
                    IncidentToAdjacentStrategy.class,
                    InlineFilterStrategy.class,
                    MatchPredicateStrategy.class,
                    OrderLimitStrategy.class,
                    OptionsStrategy.class,
                    PathProcessorStrategy.class,
                    PathRetractionStrategy.class,
                    RepeatUnrollStrategy.class,
                    ComputerVerificationStrategy.class,
                    LambdaRestrictionStrategy.class,
                    ReadOnlyStrategy.class,
                    StandardVerificationStrategy.class,
                    EarlyLimitStrategy.class,
                    EdgeLabelVerificationStrategy.class,
                    ReservedKeysVerificationStrategy.class,
                    //
                    GraphFilterStrategy.class,
                    VertexProgramStrategy.class
            ).forEach(strategy -> addDeserializer(strategy, new TrsTraversalSerializers.TraversalStrategyProxyJacksonDeserializer(strategy)));

            TrsGraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> addDeserializer(s, new TrsTraversalSerializers.TraversalStrategyProxyJacksonDeserializer(s)));
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
            return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {
            }

            @Override
            public TrsGraphSONModule create(final boolean normalize,final boolean includePropertyProperty) {
                return new GraphSONModuleV3d0(normalize,includePropertyProperty);
            }

        }
    }

    /**
     * A "builder" used to create {@link TrsGraphSONModule} instances.  Each "version" should have an associated
     * {@code GraphSONModuleBuilder} so that it can be registered with the {@link GraphSONVersion} enum.
     */
    static interface GraphSONModuleBuilder {

        /**
         * Creates a new {@link TrsGraphSONModule} object.
         *
         * @param normalize when set to true, keys and objects are ordered to ensure that they are the occur in
         *                  the same order.
         */
        TrsGraphSONModule create(final boolean normalize,final boolean includePropertyProperty);
    }
}

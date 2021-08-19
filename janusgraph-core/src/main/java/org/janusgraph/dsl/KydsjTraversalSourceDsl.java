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
package org.janusgraph.dsl;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.ArrayList;
import java.util.List;

/**
 * See {@code SocialTraversalDsl} for more information about this DSL.
 */
public class KydsjTraversalSourceDsl extends GraphTraversalSource {

    public KydsjTraversalSourceDsl(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public KydsjTraversalSourceDsl(final Graph graph) {
        super(graph);
    }

    public KydsjTraversalSourceDsl(final RemoteConnection connection) {
        super(connection);
    }

    public GraphTraversal<Vertex, Vertex> T(final String ... tids) {
        final GraphTraversalSource clone = this.clone();
        if(tids!=null&&tids.length>0) {
            List<String> graphIds = new ArrayList<>();
            Graph graph = this.getGraph();
            if (graph!=null) {
                for (String tid : tids) {
                    if (StringUtils.isNotBlank(tid)) {
                        if(graph instanceof StandardJanusGraph) {
                            String graphId = ((StandardJanusGraph) graph).getIDManager().toVertexId(tid);
                            graphIds.add(graphId);
                        }else if(graph instanceof StandardJanusGraphTx){
                            String graphId = ((StandardJanusGraphTx) graph).getIdInspector().toVertexId(tid);
                            graphIds.add(graphId);
                        }else{
                            graphIds.add(tid);
                        }
                    }
                }
            }
            Object[] graphIdArray = graphIds.toArray();
            clone.getBytecode().addStep(GraphTraversal.Symbols.V, graphIdArray);
            final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
            return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, graphIdArray));
        }else {
            clone.getBytecode().addStep(GraphTraversal.Symbols.V, tids);
            final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
            return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, tids));
        }
    }


    /**
     * Starts a traversal that finds all vertices with a "person" label and optionally allows filtering of those
     * vertices on the "name" property.
     *
     * @param names list of person names to filter on
     */
    public GraphTraversal<Vertex, Vertex> persons(String... names) {
        GraphTraversalSource clone = this.clone();

        // Manually add a "start" step for the traversal in this case the equivalent of V(). GraphStep is marked
        // as a "start" step by passing "true" in the constructor.
        clone.getBytecode().addStep(GraphTraversal.Symbols.V);
        GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        traversal.asAdmin().addStep(new GraphStep<>(traversal.asAdmin(), Vertex.class, true));

        traversal = traversal.hasLabel("person");
        if (names.length > 0) traversal = traversal.has("name", P.within(names));

        return traversal;
    }
}

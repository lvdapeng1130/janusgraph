// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.hadoop.formats.util.input.current;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.util.Constants;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetup;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KGJanusGraphHadoopSetupImpl implements JanusGraphHadoopSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    public static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    private final ModifiableHadoopConfiguration scanConf;
    private final StandardJanusGraph graph;
    private final StandardJanusGraphTx tx;
    private Set<String> vertexLabelSet;
    private Set<String> edgeLabelSet;

    public KGJanusGraphHadoopSetupImpl(final Configuration config) {
        scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);
        BasicConfiguration bc = scanConf.getJanusGraphConf();
        graph = (StandardJanusGraph) JanusGraphFactory.open(bc);
        tx = (StandardJanusGraphTx)graph.buildTransaction().readOnly().vertexCacheSize(500).start();
        String vertexLabels = config.get(Constants.GREMLIN_HADOOP_GRAPH_VERTEXLABELS);
        if(StringUtils.isNotBlank(vertexLabels)){
            this.vertexLabelSet= Arrays.stream(vertexLabels.split(",")).collect(Collectors.toSet());
        }
        String edgeLabels = config.get(Constants.GREMLIN_HADOOP_GRAPH_EDGELABELS);
        if(StringUtils.isNotBlank(edgeLabels)){
            this.edgeLabelSet=Arrays.stream(edgeLabels.split(",")).collect(Collectors.toSet());
        }
    }

    @Override
    public EdgeSerializer getEdgeSerializer(){
        return graph.getEdgeSerializer();
    }

    @Override
    public boolean vertexLabelFilter(String vertexLabel) {
        return  vertexLabelSet==null||vertexLabelSet.isEmpty()||vertexLabelSet.contains(vertexLabel);
    }

    @Override
    public boolean edgeLabelFilter(String edgeLabel) {
        return  edgeLabelSet==null||edgeLabelSet.isEmpty()||edgeLabelSet.contains(edgeLabel);
    }

    public StandardJanusGraphTx startTransaction(StandardJanusGraph graph) {
        StandardTransactionBuilder txb = graph.buildTransaction().readOnly();
        txb.setPreloadedData(true);
        txb.checkInternalVertexExistence(false);
        txb.dirtyVertexSize(0);
        txb.vertexCacheSize(0);
        return (StandardJanusGraphTx)txb.start();
    }

    @Override
    public TypeInspector getTypeInspector() {
        //Pre-load schema
        for (JanusGraphSchemaCategory sc : JanusGraphSchemaCategory.values()) {
            for (JanusGraphVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                assert k instanceof JanusGraphSchemaVertex;
                JanusGraphSchemaVertex s = (JanusGraphSchemaVertex)k;
                if (sc.hasName()) {
                    String name = s.name();
                    Preconditions.checkNotNull(name);
                }
                TypeDefinitionMap dm = s.getDefinition();
                Preconditions.checkNotNull(dm);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.IN);
            }
        }
        return tx;
    }

    @Override
    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            @Override
            public boolean isSystemType(String typeId) {
                return IDManager.isSystemRelationTypeId(typeId);
            }

            @Override
            public boolean isVertexExistsSystemType(String typeId) {
                return typeId.equals(BaseKey.VertexExists.longId());
            }

            @Override
            public boolean isVertexLabelSystemType(String typeId) {
                return typeId.equals(BaseLabel.VertexLabelEdge.longId());
            }

            @Override
            public boolean isTypeSystemType(String typeId) {
                return typeId.equals(BaseKey.SchemaCategory.longId()) ||
                        typeId.equals(BaseKey.SchemaDefinitionProperty.longId()) ||
                        typeId.equals(BaseKey.SchemaDefinitionDesc.longId()) ||
                        typeId.equals(BaseKey.SchemaName.longId()) ||
                        typeId.equals(BaseLabel.SchemaDefinitionEdge.longId());
            }
        };
    }

    @Override
    public IDManager getIDManager() {
        return graph.getIDManager();
    }

    @Override
    public RelationReader getRelationReader() {
        return graph.getEdgeSerializer();
    }

    @Override
    /**
     * 只关闭事务，而不关闭graph图库连接
     */
    public void close() {
        tx.rollback();
        if (tx.isOpen()) {
            tx.close();
        }
        graph.close();
    }

    @Override
    public boolean getFilterPartitionedVertices() {
        return scanConf.get(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES, true);
    }

    @Override
    public Serializer getDataSerializer() {
        return graph.getDataSerializer();
    }
}

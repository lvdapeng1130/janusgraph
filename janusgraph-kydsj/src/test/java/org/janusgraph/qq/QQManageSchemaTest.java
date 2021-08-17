package org.janusgraph.qq;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.kggraph.AbstractKGgraphTest;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
 */
public class QQManageSchemaTest extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(QQManageSchemaTest.class);
    private String mixedIndexConfigName="search";
    protected Configuration conf;
    protected JanusGraph graph;
    protected GraphTraversalSource g;
    @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        conf=ConfigurationUtil.loadPropertiesConfig("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\test\\resources\\trsgraph-hbase-es-test-qq.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal();
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
    @Test
    public void printSchema(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        String printSchemaStr = management.printSchema();
        LOGGER.info(printSchemaStr);
        management.rollback();
    }

    @Test
    public void deleteGraph() throws BackendException {
        if (graph != null) {
            JanusGraphFactory.drop(getJanusGraph());
        }
    }

    @Test
    public void createSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
                management.rollback();
                return;
            }
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createEdgeLabels(management);
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("eidentity_qqqun").make();
        management.makeVertexLabel("eidentity_qq").make();
    }

    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("link_member").make();
    }

    protected void createProperties(final JanusGraphManagement management) {
        //QQ和QQ群属性
        management.makePropertyKey("eidentity_qqqun").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("eidentity_qq").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("eidentity_qq_name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("person_nl").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("person_xb").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("ozfk_bqdj").dataType(String.class).cardinality(Cardinality.SET).make();

        //内置属性
        management.makePropertyKey("tid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("linktid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("linkrole").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("linktext").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("title").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("doctext").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        management.makePropertyKey("startDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("endDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("createdate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("updatedate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("status").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("attachment").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("merge_to").dataType(String.class).cardinality(Cardinality.SINGLE).make();
    }

    protected void createMixedIndexes(final JanusGraphManagement management) {
        //对象的混合索引
        management.buildIndex("eidentity_qqqun", Vertex.class)
            .addKey(management.getPropertyKey("tid"))
            .addKey(management.getPropertyKey("title"))
            .addKey(management.getPropertyKey("doctext"))
            .addKey(management.getPropertyKey("startDate"))
            .addKey(management.getPropertyKey("endDate"))
            .addKey(management.getPropertyKey("createdate"))
            .addKey(management.getPropertyKey("updatedate"))
            .addKey(management.getPropertyKey("geo"))
            .addKey(management.getPropertyKey("attachment"))
            .addKey(management.getPropertyKey("merge_to"))
            .addKey(management.getPropertyKey("status"))
            .addKey(management.getPropertyKey("role"))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey("eidentity_qqqun"))
            .indexOnly(management.getVertexLabel("eidentity_qqqun"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("eidentity_qq", Vertex.class)
            .addKey(management.getPropertyKey("tid"))
            .addKey(management.getPropertyKey("title"))
            .addKey(management.getPropertyKey("doctext"))
            .addKey(management.getPropertyKey("startDate"))
            .addKey(management.getPropertyKey("endDate"))
            .addKey(management.getPropertyKey("createdate"))
            .addKey(management.getPropertyKey("updatedate"))
            .addKey(management.getPropertyKey("geo"))
            .addKey(management.getPropertyKey("attachment"))
            .addKey(management.getPropertyKey("merge_to"))
            .addKey(management.getPropertyKey("status"))
            .addKey(management.getPropertyKey("role"))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey("eidentity_qq"))
            .addKey(management.getPropertyKey("eidentity_qq_name"))
            .addKey(management.getPropertyKey("person_nl"))
            .addKey(management.getPropertyKey("person_xb"))
            .addKey(management.getPropertyKey("ozfk_bqdj"))
            .indexOnly(management.getVertexLabel("eidentity_qq"))
            .buildKGMixedIndex(mixedIndexConfigName);
/*
        //关系的混合索引
        management.buildIndex("link_simple_mix", Edge.class).addKey(management.getPropertyKey("linktid"))
            .buildKGMixedIndex(mixedIndexConfigName);*/
    }


    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("tidUniqueIndex", Vertex.class)
            .addKey(management.getPropertyKey("tid")).buildCompositeIndex();
        //关系索引
        management.buildIndex("linkTidIndex", Edge.class)
            .addKey(management.getPropertyKey("linktid")).buildCompositeIndex();
    }
}

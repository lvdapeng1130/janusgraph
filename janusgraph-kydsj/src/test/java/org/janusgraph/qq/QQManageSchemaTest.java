package org.janusgraph.qq;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.kggraph.AbstractKGgraphTest;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.DefaultTextField;
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
        String dataPath=this.getClass().getResource("/trsgraph-hbase-es-244_es7new1.properties").getFile();
        conf=ConfigurationUtil.loadPropertiesConfig(dataPath);
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
            //createCompositeIndexes(management);
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
        management.makePropertyKey("bh").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("nl").dataType(String.class).cardinality(Cardinality.SET).make();

        //内置属性
        management.makePropertyKey(DefaultPropertyKey.TID.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LINK_TID.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LINK_TYPE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LINK_ROLE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LEFT_TID.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LEFT_TYPE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.RIGHT_TID.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.RIGHT_TYPE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.LINK_TEXT.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.TITLE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.DOC_TEXT.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();

        management.makePropertyKey(DefaultPropertyKey.START_DATE.getKey()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.END_DATE.getKey()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.CREATE_DATE.getKey()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.UPDATE_DATE.getKey()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.GEO.getKey()).dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.DSR.getKey()).dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey(DefaultPropertyKey.ROLE.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.STATUS.getKey()).dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.ATTACHMENT.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultPropertyKey.MEDIASET.getKey()).dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey(DefaultPropertyKey.NOTESET.getKey()).dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey(DefaultPropertyKey.MERGE_TO.getKey()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
    }

    protected void createMixedIndexes(final JanusGraphManagement management) {
        //对象的混合索引
        management.buildIndex("eidentity_qqqun", Vertex.class)
            .addKey(management.getPropertyKey(DefaultPropertyKey.TID.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.TITLE.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.DOC_TEXT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.START_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.END_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.CREATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.UPDATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.GEO.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.ATTACHMENT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.NOTESET.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.NOTESET.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.MERGE_TO.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.STATUS.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.ROLE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.DSR.getKey()))
            .addKey(management.getPropertyKey("eidentity_qqqun"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .indexOnly(management.getVertexLabel("eidentity_qqqun"))
            .buildKGMixedIndex(mixedIndexConfigName,"kg4_76076_object_entity_eidentity_qqqun");
        management.buildIndex("eidentity_qq", Vertex.class)
            .addKey(management.getPropertyKey(DefaultPropertyKey.TID.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.TITLE.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.DOC_TEXT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.START_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.END_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.CREATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.UPDATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.GEO.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.ATTACHMENT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.NOTESET.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.NOTESET.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey(DefaultPropertyKey.MERGE_TO.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.STATUS.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.ROLE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.DSR.getKey()))
            .addKey(management.getPropertyKey("eidentity_qq"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey("eidentity_qq_name"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey("person_nl"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey("person_xb"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .addKey(management.getPropertyKey("ozfk_bqdj"), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .indexOnly(management.getVertexLabel("eidentity_qq"))
            .buildKGMixedIndex(mixedIndexConfigName,"kg4_76076_object_entity_eidentity_qq");


        /*checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LINK_TID, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.CREATE_DATE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.UPDATE_DATE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LINK_ROLE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LINK_TEXT, "ik_max_word");
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LINK_TYPE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LEFT_TID, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.LEFT_TYPE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.RIGHT_TID, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.RIGHT_TYPE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.START_DATE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.END_DATE, null);
        checkDefaultKey(propertyKeys, indexName, DefaultPropertyKey.DSR, null);*/
        //关系的混合索引
        management.buildIndex("link_member", Edge.class)
            .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TID.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.CREATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.UPDATE_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_ROLE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TYPE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.LEFT_TID.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.LEFT_TYPE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.RIGHT_TID.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.RIGHT_TYPE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.START_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.END_DATE.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.DSR.getKey()))
            .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TEXT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
            .indexOnly(management.getEdgeLabel("link_member"))
            .buildKGMixedIndex(mixedIndexConfigName);
/*
        //关系的混合索引
        management.buildIndex("link_simple_mix", Edge.class).addKey(management.getPropertyKey("linktid"))
            .buildKGMixedIndex(mixedIndexConfigName);*/
    }


    @Test
    public void createMixLink(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            LOGGER.info("creating  link es schema");
            management.buildIndex("link_member", Edge.class)
                .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TID.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.CREATE_DATE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.UPDATE_DATE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_ROLE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TYPE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.LEFT_TID.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.LEFT_TYPE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.RIGHT_TID.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.RIGHT_TYPE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.START_DATE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.END_DATE.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.DSR.getKey()))
                .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TEXT.getKey()), Mapping.TEXTSTRING.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
                .indexOnly(management.getEdgeLabel("link_member"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }


    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("tidUniqueIndex", Vertex.class)
            .addKey(management.getPropertyKey("tid")).buildCompositeIndex();
        //关系索引
        management.buildIndex("linkTidIndex", Edge.class)
            .addKey(management.getPropertyKey("linktid")).buildCompositeIndex();
    }

    @Test
    public void insertSimple5(){
        insertSimple3("tid1","tid2","link1");
    }

    public void insertSimple3(String tid,String tid2,String linkId){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("eidentity_qq")
/*                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "32222",
                    "geo", Geoshape.point(23.22, 113.1122))*/
                .property(DefaultTextField.TITLE.getName(),"我是测试QQ")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            GraphTraversal<Vertex, Vertex> qqTraversal2 = threadedTx.traversal()
                .addV("eidentity_qqqun")
/*                .property(DefaultFields.UPDATEDATE.getName(),new Date(),"startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "11111",
                    "geo", Geoshape.point(24.22, 134.1122))*/
                .property(DefaultTextField.TITLE.getName(),"我是测试QQ群")
                .property(T.id, tid2);
            Vertex qq2 = qqTraversal2.next();

            String graphId1 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            String graphId2 = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid2);
            Edge next = threadedTx.traversal().V(graphId1).as("a")
                .V(graphId2)
                .addE("link_member")
                .property(T.id,linkId)
                .property("link_tid", linkId)
                .property("link_type","link_member")
                .property("left_tid", graphId2)
                .property("right_tid", graphId1)
                .to("a").next();
            Object id = next.id();
            threadedTx.commit();
        }
    }

    @Test
    public void insertSimple(){
        try(StandardJanusGraphTx threadedTx = (StandardJanusGraphTx) this.getJanusGraph().buildTransaction()
            .consistencyChecks(true)
            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start()) {
            String tid="tid0014";
            String graphId = ((StandardJanusGraph) this.getJanusGraph()).getIDManager().toVertexId(tid);
            GraphTraversal<Vertex, Vertex> qqTraversal = threadedTx.traversal()
                .addV("eidentity_qq")
                .property("eidentity_qq","123456789","startDate", new Date(),
                    "endDate", new Date(),
                    "dsr", "我是123456789的dsr内置属性",
                    "geo", Geoshape.point(24.22, 134.1122))
                .property(DefaultTextField.TITLE.getName(),"我是测试标签")
                .property(T.id, tid);
            Vertex qq = qqTraversal.next();
            threadedTx.commit();
        }
    }
}

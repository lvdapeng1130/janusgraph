package org.janusgraph.kggraph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
 */
public class ManageSchemaTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageSchemaTest.class);
    private String mixedIndexConfigName="search";
    @Test
    public void printSchema(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        String printSchemaStr = management.printSchema();
        LOGGER.info(printSchemaStr);
        management.rollback();
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
        management.makeVertexLabel("object_qq").make();
        management.makeVertexLabel("object_qqqun").make();
    }

    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("link_simple").make();
    }

    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("grade").dataType(Integer.class).make();
        management.makePropertyKey("qq_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("qqqun_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("text").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("time").dataType(Date.class).make();
        management.makePropertyKey("age1").dataType(Integer.class).make();
        management.makePropertyKey("linktid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("tid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("merge_to").dataType(Long.class).cardinality(Cardinality.SINGLE).make();

        management.makePropertyKey("startDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("endDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("geo").dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("dsr").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("status").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("attachment").dataType(String.class).cardinality(Cardinality.SINGLE).make();
    }

    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("tid_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("tid")).buildCompositeIndex();
        management.buildIndex("qqqun_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qqqun_num")).buildCompositeIndex();
        management.buildIndex("qq_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qq_num")).buildCompositeIndex();
        //关系索引
        management.buildIndex("eget_link_tid", Edge.class)
            .addKey(management.getPropertyKey("linktid")).buildCompositeIndex();
    }

    protected void createMixedIndexes(final JanusGraphManagement management) {
        //对象的混合索引
        management.buildIndex("object_qq", Vertex.class)
            .addKey(management.getPropertyKey("name"))
            .addKey(management.getPropertyKey("grade"))
            .addKey(management.getPropertyKey("qq_num"))
            .addKey(management.getPropertyKey("time"))
            .addKey(management.getPropertyKey("age1"))
            .addKey(management.getPropertyKey("tid"))
            .indexOnly(management.getVertexLabel("object_qq"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("object_qqqun", Vertex.class)
            .addKey(management.getPropertyKey("name"))
            .addKey(management.getPropertyKey("time"))
            .addKey(management.getPropertyKey("qqqun_num"))
            .addKey(management.getPropertyKey("text"))
            .addKey(management.getPropertyKey("tid"))
            .indexOnly(management.getVertexLabel("object_qqqun"))
            .buildKGMixedIndex(mixedIndexConfigName);

            //关系的混合索引
        management.buildIndex("link_simple_mix", Edge.class).addKey(management.getPropertyKey("linktid"))
                .buildKGMixedIndex(mixedIndexConfigName);
    }

}

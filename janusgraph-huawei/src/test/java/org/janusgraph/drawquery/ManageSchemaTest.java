package org.janusgraph.drawquery;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.qq.DefaultPropertyKey;
import org.janusgraph.util.system.DefaultFields;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 构建海关测试数据图库结构
 */
public class ManageSchemaTest extends AbstractKGgraphTest {
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
        for(int i=0;i<10;i++) {
            management.makeVertexLabel("objLabel"+i).make();
        }
    }

    protected void createEdgeLabels(final JanusGraphManagement management) {
        for(int i=0;i<10;i++) {
            management.makeEdgeLabel("linkLabel"+i).make();
        }
    }

    protected void createProperties(final JanusGraphManagement management) {
        //测试1
        management.makePropertyKey("cs_name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("cs_number").dataType(Integer.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("cs_date").dataType(Date.class).cardinality(Cardinality.SET).make();

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
        this.createMixedIndexesByTest1(management);
    }
    private void createMixedIndexesByTest1(JanusGraphManagement management) {
        for(int i=0;i<10;i++) {
            String objLabel="objLabel"+i;
            management.buildIndex(objLabel, Vertex.class)
                .addKey(management.getPropertyKey("cs_name"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey("cs_number"))
                .addKey(management.getPropertyKey("cs_date"))
                .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
                .addKey(management.getPropertyKey("dsr"))
                .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()), ParameterType.MAPPING.getParameter(Mapping.STRING))
                .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .indexOnly(management.getVertexLabel(objLabel))
                .buildKGMixedIndex(mixedIndexConfigName);
        }
        for(int i=0;i<10;i++) {
            String linkType="linkLabel"+i;
            management.buildIndex(linkType, Edge.class)
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
                .addKey(management.getPropertyKey(DefaultPropertyKey.LINK_TEXT.getKey()), Mapping.TEXTSTRING.asParameter(),
                    Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"))
                .indexOnly(management.getEdgeLabel(linkType))
                .buildKGMixedIndex(mixedIndexConfigName);
        }
    }

}

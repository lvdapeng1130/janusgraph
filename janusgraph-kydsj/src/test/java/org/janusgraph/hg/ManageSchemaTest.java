package org.janusgraph.hg;

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
    public void createSchema1() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            LOGGER.info("creating schema");
            management.makeVertexLabel("qy_e5").make();
            management.makeVertexLabel("gj_e5").make();
            management.makeEdgeLabel("link_scqy_e5").make();
            management.makeEdgeLabel("link_t1_e5").make();
            management.makeEdgeLabel("link_t2_e5").make();
            management.buildIndex("qy_e5", Vertex.class)
                .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
                .addKey(management.getPropertyKey("dsr"))
                .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
                .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .indexOnly(management.getVertexLabel("qy_e5"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.buildIndex("gj_e5", Vertex.class)
                .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
                .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
                .addKey(management.getPropertyKey("dsr"))
                .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
                .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .indexOnly(management.getVertexLabel("gj_e5"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.buildIndex("link_scqy_e5", Edge.class)
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
                .indexOnly(management.getEdgeLabel("link_scqy_e5"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.buildIndex("link_t1_e5", Edge.class)
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
                .indexOnly(management.getEdgeLabel("link_t1_e5"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.buildIndex("link_t2_e5", Edge.class)
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
                .indexOnly(management.getEdgeLabel("link_t2_e5"))
                .buildKGMixedIndex(mixedIndexConfigName);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
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
        //测试1
        management.makeVertexLabel("hw_e1").make();
        management.makeVertexLabel("qy_e1").make();

        //测试2
        management.makeVertexLabel("gr_e2").make();
        management.makeVertexLabel("hb_e2").make();
        management.makeVertexLabel("ka_e2").make();

        //测试3
        management.makeVertexLabel("qy_e3").make();
        management.makeVertexLabel("v1_e3").make();
        management.makeVertexLabel("v2_e3").make();

        //测试4
        management.makeVertexLabel("qy_e4").make();
        management.makeVertexLabel("hw_e4").make();
        management.makeVertexLabel("zscqbaxx_e4").make();

        //测试5
        management.makeVertexLabel("hw_e5").make();
        management.makeVertexLabel("bgd_e5").make();
        management.makeVertexLabel("ka_e5").make();

        //测试6
        management.makeVertexLabel("hb_e6").make();

        //测试7
        management.makeVertexLabel("hb_e7").make();

    }

    protected void createEdgeLabels(final JanusGraphManagement management) {
        //测试1
        management.makeEdgeLabel("link_jkhw_e1").make();

        //测试2
        management.makeEdgeLabel("link_cz_e2").make();
        management.makeEdgeLabel("link_mdjc_e2").make();

        //测试3
        management.makeEdgeLabel("link1_e3").make();
        management.makeEdgeLabel("link2_e3").make();
        management.makeEdgeLabel("link3_e3").make();

        //测试4
        management.makeEdgeLabel("link_scxsdw_e4").make();
        management.makeEdgeLabel("link_zscqqq_e4").make();
        management.makeEdgeLabel("link_qlr_e4").make();

        //测试5
        management.makeEdgeLabel("link_sbhw_e5").make();
        management.makeEdgeLabel("link_zyg_e5").make();
    }

    protected void createProperties(final JanusGraphManagement management) {
        //测试1
        management.makePropertyKey("spbhhs").dataType(String.class).cardinality(Cardinality.SET).make();

        //测试4
        management.makePropertyKey("bgdbh_e4").dataType(String.class).cardinality(Cardinality.SET).make();

        //测试5
        management.makePropertyKey("spbhhs_e5").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("bgdbh_e5").dataType(String.class).cardinality(Cardinality.SET).make();

        //测试6
        management.makePropertyKey("jcj_e6").dataType(String.class).cardinality(Cardinality.SET).make();

        //测试7
        management.makePropertyKey("hbrq_e7").dataType(Date.class).cardinality(Cardinality.SET).make();

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

    protected void createCompositeIndexes(final JanusGraphManagement management) {
        management.buildIndex("tid_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("tid")).buildCompositeIndex();
        management.buildIndex("qqqun_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qqqun_num")).buildCompositeIndex();
        management.buildIndex("qq_num_composite_index", Vertex.class)
            .addKey(management.getPropertyKey("qq_num")).buildCompositeIndex();
        //关系索引
        management.buildIndex("eget_link_tid", Edge.class)
            .addKey(management.getPropertyKey("link_tid")).buildCompositeIndex();
    }

    protected void createMixedIndexes(final JanusGraphManagement management) {
        //测试1
        this.createMixedIndexesByTest1(management);
        //测试2
        this.createMixedIndexesByTest2(management);
        //测试3
        this.createMixedIndexesByTest3(management);
        //测试4
        this.createMixedIndexesByTest4(management);
        //测试5
        this.createMixedIndexesByTest5(management);
        //测试6
        this.createMixedIndexesByTest6(management);
        //测试7
        this.createMixedIndexesByTest7(management);
    }

    private void createMixedIndexesByTest7(JanusGraphManagement management) {
        management.buildIndex("hb_e7", Vertex.class)
            .addKey(management.getPropertyKey("hbrq_e7"))
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hb_e7"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest6(JanusGraphManagement management) {
        management.buildIndex("hb_e6", Vertex.class)
            .addKey(management.getPropertyKey("jcj_e6"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hb_e6"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest5(JanusGraphManagement management) {
        management.buildIndex("hw_e5", Vertex.class)
            .addKey(management.getPropertyKey("spbhhs_e5"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey("bgdbh_e5"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hw_e5"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("bgd_e5", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("bgd_e5"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("ka_e5", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("ka_e5"))
            .buildKGMixedIndex(mixedIndexConfigName);

        management.buildIndex("link_sbhw_e5", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_sbhw_e5"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_zyg_e5", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_zyg_e5"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest4(JanusGraphManagement management) {
        management.buildIndex("qy_e4", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("qy_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("hw_e4", Vertex.class)
            .addKey(management.getPropertyKey("bgdbh_e4"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hw_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("zscqbaxx_e4", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("zscqbaxx_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);

        management.buildIndex("link_scxsdw_e4", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_scxsdw_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_zscqqq_e4", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_zscqqq_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_qlr_e4", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_qlr_e4"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest3(JanusGraphManagement management) {
        management.buildIndex("qy_e3", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("qy_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("v1_e3", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("v1_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("v2_e3", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("v2_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);

        management.buildIndex("link1_e3", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link1_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link2_e3", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link2_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link3_e3", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link3_e3"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest2(JanusGraphManagement management) {
        management.buildIndex("gr_e2", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("gr_e2"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("hb_e2", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hb_e2"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("ka_e2", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("ka_e2"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_cz_e2", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_cz_e2"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_mdjc_e2", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_mdjc_e2"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

    private void createMixedIndexesByTest1(JanusGraphManagement management) {
        management.buildIndex("hw_e1", Vertex.class)
            .addKey(management.getPropertyKey("spbhhs"), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("hw_e1"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("qy_e1", Vertex.class)
            .addKey(management.getPropertyKey(DefaultFields.STARTDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.UPDATEDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.ENDDATE.getName()))
            .addKey(management.getPropertyKey(DefaultFields.GEO.getName()))
            .addKey(management.getPropertyKey("dsr"))
            .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
            .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.MEDIASET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .addKey(management.getPropertyKey(DefaultFields.NOTESET.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
            .indexOnly(management.getVertexLabel("qy_e1"))
            .buildKGMixedIndex(mixedIndexConfigName);
        management.buildIndex("link_jkhw_e1", Edge.class)
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
            .indexOnly(management.getEdgeLabel("link_jkhw_e1"))
            .buildKGMixedIndex(mixedIndexConfigName);
    }

}

package org.janusgraph.kggraph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.TransactionalConfiguration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.hadoop.MapReduceIndexManagement;
import org.janusgraph.util.system.DefaultFields;
import org.janusgraph.util.system.DefaultKeywordField;
import org.janusgraph.util.system.DefaultTextField;
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
        boolean startDate = DefaultFields.isDefaultField("startDate");
      /*  try {
            Thread.sleep(1000*60*60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        management.rollback();
    }
    @Test
    public void iterateIndexes() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        JanusGraphIndex index=management.getGraphIndex("person");
        String pattern = "%-30s | %-11s | %-9s | %-14s | %-10s %10s |%n";
        StringBuilder sb = new StringBuilder();
        String type = getIndexType(index);
        PropertyKey[] keys = index.getFieldKeys();
        String[][] keyStatus = getKeyStatus(keys, index);
        sb.append(String.format(pattern, index.name(), type, index.isUnique(), index.getBackingIndex(), keyStatus[0][0] + ":", keyStatus[0][1]));
        if (keyStatus.length > 1) {
            for (int i = 1; i < keyStatus.length; i++) {
                sb.append(String.format(pattern, "", "", "", "", keyStatus[i][0] + ":", keyStatus[i][1]));
            }
        }
        management.rollback();
        LOGGER.info(sb.toString());
    }

    @Test
    public void removeIndexDuplicateFields() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        management.removeIndexDuplicateFields("person");
        management.commit();
    }

    private String getIndexType(JanusGraphIndex index) {
        String type;
        if (index.isCompositeIndex()) {
            type = "Composite";
        } else if (index.isMixedIndex()) {
            type = "Mixed";
        } else {
            type = "Unknown";
        }
        return type;
    }

    private String[][] getKeyStatus(PropertyKey[] keys, JanusGraphIndex index) {
        String[][] keyStatus = new String[keys.length][2];
        for (int i = 0; i < keys.length; i++) {
            keyStatus[i][0] = keys[i].name();
            keyStatus[i][1] = index.getIndexStatus(keys[i]).name();
        }
        return keyStatus.length > 0 ? keyStatus: new String[][] {{"",""}};
    }

    @Test
    public void printConfiguration(){
        GraphDatabaseConfiguration configuration = ((StandardJanusGraph) getJanusGraph()).getConfiguration();
        Configuration configuration1 = configuration.getConfiguration();
        System.out.println(configuration1);
    }

    @Test
    public void setConfiguration(){
        //GraphDatabaseConfiguration configuration = ((StandardJanusGraph) getJanusGraph()).getConfiguration();
        KCVSConfiguration globalSystemConfig = ((StandardJanusGraph) getJanusGraph()).getBackend().getGlobalSystemConfig();
        TransactionalConfiguration transactionalConfig = new TransactionalConfiguration(globalSystemConfig);
        transactionalConfig.set("index.search.hostname","192.168.5.124:9200,192.168.5.124:9201,192.168.5.125:9200,192.168.5.125:9201,192.168.5.126:9200,192.168.5.126:9201");
        transactionalConfig.commit();
    }

    @Test
    public void deleteGraph() throws BackendException {
        if (graph != null) {
            JanusGraphFactory.drop(getJanusGraph());
        }
    }

    @Test
    public void disableIndex(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            if(object_qq!=null){
                management.updateIndex(object_qq, SchemaAction.DISABLE_INDEX).get();
            }
            management.commit();
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(graph, "object_qq").status(SchemaStatus.DISABLED).call();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void registerIndex(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            if(object_qq!=null){
                management.updateIndex(object_qq, SchemaAction.REGISTER_INDEX).get();
            }
            management.commit();
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(graph, "object_qq").status(SchemaStatus.DISABLED).call();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void enableIndex(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            if(object_qq!=null){
                management.updateIndex(object_qq, SchemaAction.REGISTER_INDEX).get();
            }
            management.commit();
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(graph, "object_qq").status(SchemaStatus.DISABLED).call();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void deleteIndex(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            if(object_qq!=null){
                management.updateIndex(object_qq, SchemaAction.DISABLE_INDEX).get();
            }
            management.commit();
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(graph, "object_qq").status(SchemaStatus.DISABLED).call();
            // Delete the index using MapReduceIndexJobs
            management = getJanusGraph().openManagement();
            MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
            JanusGraphManagement.IndexJobFuture future = mr.updateIndex(management.getGraphIndex("object_qq"), SchemaAction.REMOVE_INDEX);
            management.commit();
            ScanMetrics scanMetrics = future.get();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void deleteIndex2(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
           /* JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            if(object_qq!=null){
                management.updateIndex(object_qq, SchemaAction.DISABLE_INDEX).get();
            }
            management.commit();*/
            // Block until the SchemaStatus transitions from INSTALLED to REGISTERED
            ManagementSystem.awaitGraphIndexStatus(getJanusGraph(), "object_qq").status(SchemaStatus.DISABLED).call();
            management = getJanusGraph().openManagement();
            JanusGraphIndex object_qq = management.getGraphIndex("object_qq");
            management.updateIndex(object_qq, SchemaAction.REMOVE_INDEX).get();
            management.commit();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void deleteIndex3(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            management.deleteMixedIndex("object_qq");
            management.commit();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void createObjectLabel(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            management.makeVertexLabel("object_qq").make();
            String s = management.printVertexLabels();
            LOGGER.info(s);
            management.commit();
            this.printSchema();
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
            createCompositeIndexes(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void createSchema1() {
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
            //createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    @Test
    public void createSchema2() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            createMixedIndexes(management);
            management.commit();
            Thread.sleep(1000*60*60);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("object_qq").make();
        management.makeVertexLabel("object_qqqun").make();
        management.makeVertexLabel("test").make();
    }

    protected void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("link_simple").make();
        management.makeEdgeLabel("test").make();
    }

    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("db").dataType(Double.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("grade").dataType(Integer.class).make();
        management.makePropertyKey("qq_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("qqqun_num").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("text").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("time").dataType(Date.class).make();
        management.makePropertyKey("age1").dataType(Integer.class).make();
        management.makePropertyKey("link_tid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.LINK_TYPE.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.LEFT_TID.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.RIGHT_TID.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultKeywordField.TID.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.TITLE.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.RIGHT_TYPE.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.LEFT_TYPE.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("merge_to").dataType(Long.class).cardinality(Cardinality.SINGLE).make();

        management.makePropertyKey(DefaultFields.STARTDATE.getName()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.UPDATEDATE.getName()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.ENDDATE.getName()).dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.GEO.getName()).dataType(Geoshape.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.DSR.getName()).dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("role").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey("status").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.ATTACHMENT.getName()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        management.makePropertyKey(DefaultFields.MEDIASET.getName()).dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey(DefaultFields.NOTESET.getName()).dataType(String.class).cardinality(Cardinality.SET).make();
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

    @Test
    public void createMixedIndex(){
        JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            management.buildIndex("object_qq", Vertex.class)
                .addKey(management.getPropertyKey("name"))
                .addKey(management.getPropertyKey("grade"))
                .addKey(management.getPropertyKey("qq_num"))
                .addKey(management.getPropertyKey("time"))
                .addKey(management.getPropertyKey("db"))
                .addKey(management.getPropertyKey("age1"))
                .addKey(management.getPropertyKey("dsr"))
                .addKey(management.getPropertyKey(DefaultKeywordField.TID.getName()),ParameterType.MAPPING.getParameter(Mapping.STRING))
                .addKey(management.getPropertyKey(DefaultTextField.TITLE.getName()), ParameterType.TEXT_ANALYZER.getParameter("ik_max_word"),
                    ParameterType.MAPPING.getParameter(Mapping.TEXTSTRING))
                .indexOnly(management.getVertexLabel("object_qq"))
                .buildKGMixedIndex(mixedIndexConfigName,"fmb_000xxx");
            management.commit();
            this.printSchema();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    protected void createMixedIndexes(final JanusGraphManagement management) {
        //对象的混合索引
        management.buildIndex("object_qq", Vertex.class)
            .addKey(management.getPropertyKey("name"))
            .addKey(management.getPropertyKey("grade"))
            .addKey(management.getPropertyKey("qq_num"))
            .addKey(management.getPropertyKey("time"))
            .addKey(management.getPropertyKey("db"))
            .addKey(management.getPropertyKey("age1"))
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
            .indexOnly(management.getVertexLabel("object_qq"))
            .buildKGMixedIndex(mixedIndexConfigName,"fmb_000xxx");
        management.buildIndex("object_qqqun", Vertex.class)
            .addKey(management.getPropertyKey("name"))
            .addKey(management.getPropertyKey("time"))
            .addKey(management.getPropertyKey("qqqun_num"))
            .addKey(management.getPropertyKey("text"))
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
            .indexOnly(management.getVertexLabel("object_qqqun"))
            .buildKGMixedIndex(mixedIndexConfigName,"wqq");

            //关系的混合索引
        management.buildIndex("link_simple", Edge.class)
            .addKey(management.getPropertyKey("link_tid"))
            .addKey(management.getPropertyKey("left_tid"))
            .addKey(management.getPropertyKey("link_type"))
            .addKey(management.getPropertyKey("right_tid"))
            .addKey(management.getPropertyKey("dsr"))
            .indexOnly(management.getEdgeLabel("link_simple"))
                .buildKGMixedIndex(mixedIndexConfigName);
            //关系的混合索引
        management.buildIndex("test", Edge.class)
            .addKey(management.getPropertyKey("link_tid"))
            .addKey(management.getPropertyKey("link_type"))
            .addKey(management.getPropertyKey("left_tid"))
            .addKey(management.getPropertyKey("right_tid"))
            .addKey(management.getPropertyKey("dsr"))
            .indexOnly(management.getEdgeLabel("test"))
                .buildKGMixedIndex(mixedIndexConfigName);
    }

}

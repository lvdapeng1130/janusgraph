package org.janusgraph.cs;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
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
    public void printOpenTrans(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        Set<String> openInstances = management.getOpenInstances();
        for(String open:openInstances) {
            LOGGER.info(open);
        }
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
            LOGGER.info("creating schema");
            createProperties(management);
            createVertexLabels(management);
            createMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    protected void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("object_qq_test").make();
    }

    protected void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("xc").dataType(String.class).cardinality(Cardinality.SET).make();
        management.makePropertyKey("xs").dataType(String.class).cardinality(Cardinality.SET).make();
    }


    protected void createMixedIndexes(final JanusGraphManagement management) {
        //对象的混合索引
        management.buildIndex("object_qq_test", Vertex.class)
            .addKey(management.getPropertyKey("xc"))
            .addKey(management.getPropertyKey("xs"))
            .indexOnly(management.getVertexLabel("object_qq_test"))
            .buildKGMixedIndex(mixedIndexConfigName,"fmb_000xxx");
    }

    @Test
    public void updateSchema() {
        final JanusGraphManagement management = getJanusGraph().openManagement();
        try {
            LOGGER.info("update schema");
            updateMixedIndexes(management);
            management.commit();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
            management.rollback();
        }
    }

    private void updateMixedIndexes(final JanusGraphManagement management){
        management.makePropertyKey("xc_update").dataType(String.class).cardinality(Cardinality.SET).make();
        JanusGraphIndex graphIndex = management.getGraphIndex("object_qq_test");
        PropertyKey propKey = management.getPropertyKey("xc_update");
        management.addKGIndexKey(graphIndex, propKey, Mapping.TEXT.asParameter(), Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"));
    }

}

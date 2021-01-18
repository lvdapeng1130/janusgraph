package org.janusgraph.kggraph;

import org.janusgraph.core.schema.JanusGraphManagement;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: ldp
 * @time: 2021/1/18 14:46
 * @jira:
 */
public class ManageSchemaTest extends AbstractKGgraphTest{
    private static final Logger LOGGER = LoggerFactory.getLogger(ManageSchemaTest.class);
   /* @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-test.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal();
    }
    private JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }*/
    @Test
    public void printSchema(){
        final JanusGraphManagement management = getJanusGraph().openManagement();
        String printSchemaStr = management.printSchema();
        LOGGER.info(printSchemaStr);
        management.rollback();
    }
}

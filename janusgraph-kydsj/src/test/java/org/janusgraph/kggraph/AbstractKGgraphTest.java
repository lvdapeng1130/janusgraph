package org.janusgraph.kggraph;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author: ldp
 * @time: 2021/1/18 14:53
 * @jira:
 */
public class AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKGgraphTest.class);
    protected Configuration conf;
    protected JanusGraph graph;
    protected GraphTraversalSource g;
    @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-test.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal();
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

package org.janusgraph.cs;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.dsl.KydsjTraversalSource;
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
    protected KydsjTraversalSource g;
    @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        conf = new PropertiesConfiguration("C:\\work\\kggraph\\trunk\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-kg4.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal(KydsjTraversalSource.class);
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

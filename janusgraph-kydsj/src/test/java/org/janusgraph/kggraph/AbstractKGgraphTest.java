package org.janusgraph.kggraph;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.util.system.ConfigurationUtil;
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
        conf = ConfigurationUtil.loadPropertiesConfig("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-test.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal(KydsjTraversalSource.class);
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

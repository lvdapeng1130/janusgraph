package org.janusgraph.cs;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void startHBase() throws ConfigurationException {
        LOGGER.info("opening graph");
        conf = ConfigurationUtil.loadPropertiesConfig("C:\\work\\kggraph\\trunk\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-244_es7.properties");
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal(KydsjTraversalSource.class);
    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

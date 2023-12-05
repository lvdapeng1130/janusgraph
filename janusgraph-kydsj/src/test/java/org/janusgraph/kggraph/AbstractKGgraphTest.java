package org.janusgraph.kggraph;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.script.ScriptException;

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

    protected static GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
    @Before
    public void startHBase() throws IOException, ConfigurationException, ScriptException {
        LOGGER.info("opening graph");
        String dataPath=this.getClass().getResource("/trsgraph-hbase-es-244_es7new.properties").getFile();
        conf = ConfigurationUtil.loadPropertiesConfig(dataPath);
        graph = JanusGraphFactory.open(conf);
        g = graph.traversal(KydsjTraversalSource.class);
        String gremlin = "g.V().limit(1)";
        engine.compile(gremlin);
        //engine.compile(gremlin);

    }
    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

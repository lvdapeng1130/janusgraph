package org.janusgraph.kggraph;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.dsl.KydsjTraversalSource;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: ldp
 * @time: 2021/1/18 14:53
 * @jira:
 */
public class AbstractKGgraphTest2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKGgraphTest2.class);
    protected Configuration conf;
    protected JanusGraph graph;
    protected KydsjTraversalSource g;

    @Before
    public void startHBase() throws IOException, ConfigurationException {
        LOGGER.info("opening graph");
        Map<String, String> confProperty = new HashMap<>();
        confProperty.put("graph.zookeeper-uri", "192.168.5.121:2181,192.168.5.122:2181,192.168.5.123:2181");
        confProperty.put("index.search.elasticsearch.el-cs-socket-timeout", "60000");
        confProperty.put("graph.zookeeper-graph-node", "kg4_00084");
        confProperty.put("index.search.elasticsearch.use-all-field", "false");
        confProperty.put(" storage.hbase.compat-class", "org.janusgraph.diskstorage.hbase.HBaseCompat1_0");
        confProperty.put("storage.backend", "hbase");
        confProperty.put("graph.set-vertex-id", "true");
        confProperty.put("storage.hbase.table", "kg4_00084");
        confProperty.put("storage.port", "2181");
        confProperty.put("storage.buffer-size", "102400");
        confProperty.put("index.search.index-name", "kg4_00084");
        //confProperty.put("index.search.hostname", "192.168.5.124:9200,192.168.5.124:9201,192.168.5.125:9200,192.168.5.125:9201,192.168.5.126:9200,192.168.5.126:9201");
        confProperty.put("index.search.hostname", "192.168.5.124:9400");
        confProperty.put("index.search.elasticsearch.client-only", "true");
        confProperty.put("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        confProperty.put("janusgraphmr.ioformat.conf.storage.hostname", "192.168.5.121,192.168.5.122,192.168.5.123");
        confProperty.put("index.search.elasticsearch.retry_on_conflict", "100000");
        confProperty.put("janusgraphmr.ioformat.conf.graph.zookeeper-sessionTimeoutMs", "15000");
        confProperty.put("index.search.elasticsearch.el-cs-connect-timeout", "50000");
        confProperty.put("janusgraphmr.ioformat.conf.storage.hbase.table", "kg4_00084");
        confProperty.put("janusgraphmr.ioformat.conf.storage.port", "2181");
        confProperty.put("index.search.elasticsearch.el-cs-retry-timeout", "60000");
        confProperty.put("janusgraphmr.ioformat.conf.storage.backend", "hbase");
        confProperty.put("storage.hostname", "192.168.5.121,192.168.5.122,192.168.5.123");
        confProperty.put("janusgraphmr.ioformat.conf.graph.zookeeper-uri", "192.168.5.121:2181,192.168.5.122:2181,192.168.5.123:2181");
        confProperty.put("janusgraphmr.ioformat.conf.graph.zookeeper-connectionTimeoutMs", "15000");
        confProperty.put("storage.batch-loading", "false");
        confProperty.put("storage.hbase.regions-per-server", "2");
        confProperty.put("index.search.backend", "elasticsearch");
        confProperty.put("janusgraphmr.ioformat.conf.graph.zookeeper-graph-node", "kg4_00084");
        this.conf = new MapConfiguration(confProperty);
        PropertiesConfiguration propertiesConfiguration=new PropertiesConfiguration();
        for(Map.Entry<String, String> entry:confProperty.entrySet()){
            String key =entry.getKey();
            String value=entry.getValue();
            String[] array = value.split(",");
            if(array.length>1){
                propertiesConfiguration.setProperty(key, array);
            }else {
                propertiesConfiguration.setProperty(key, value);
            }
        }
        //conf = ConfigurationUtil.loadPropertiesConfig("C:\\work\\kggraph\\trunk\\janusgraph-kydsj\\src\\main\\resources\\trsgraph-hbase-es-130_test.properties");
        graph = JanusGraphFactory.open(propertiesConfiguration);
        g = graph.traversal(KydsjTraversalSource.class);
    }

    protected JanusGraph getJanusGraph() {
        return (JanusGraph) graph;
    }
}

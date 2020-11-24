package org.janusgraph.kydsj;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;

/**
 * @author: ldp
 * @time: 2020/8/5 15:28
 * @jira:
 */
public class IndexTest {
    protected Configuration conf;
    public JanusGraph openGraph() throws ConfigurationException {
        conf = new PropertiesConfiguration("D:\\github\\janusgraph\\janusgraph-kydsj\\src\\main\\resources\\jgex-hbase-es.properties");
        JanusGraph open = JanusGraphFactory.open(conf);
        return open;
    }
    public static void main(String[] args) throws ConfigurationException {
        IndexTest indexTest=new IndexTest();
        JanusGraph janusGraph = indexTest.openGraph();
        JanusGraphManagement management = janusGraph.openManagement();
        System.out.println(management.printSchema());
        management.rollback();
        management = janusGraph.openManagement();
        String indexName="testIndex";
        management.buildIndex(indexName, Vertex.class)
            .addKey(management.getPropertyKey("age"))
            .addKey(management.getPropertyKey("time"))
            .addKey(management.getPropertyKey("text"))
            .addKey(management.getPropertyKey("place"))
            .addKey(management.getPropertyKey("name"))
            .buildMixedIndex("search");
        management.commit();
        indexTest.awaitIndexEnableAndReIndex(janusGraph,indexName);
        System.out.println("完成！！！！");
    }
    private void awaitIndexEnableAndReIndex(JanusGraph janusGraph,String indexName) throws RuntimeException {
        try {
            //设置index状态enable
            ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).timeout(60, ChronoUnit.MINUTES).status(SchemaStatus.REGISTERED).call();
            JanusGraphManagement management = janusGraph.openManagement();
            management.updateIndex(management.getGraphIndex(indexName), SchemaAction.ENABLE_INDEX).get();
            management.commit();
            //Wait for the index to become available
            ManagementSystem.awaitGraphIndexStatus(janusGraph, indexName).timeout(60, ChronoUnit.MINUTES).status(SchemaStatus.ENABLED).call();
            //Reindex the existing data
            management = janusGraph.openManagement();
            management.updateIndex(management.getGraphIndex(indexName), SchemaAction.REINDEX).get();
            management.commit();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}

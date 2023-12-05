package org.janusgraph.kggraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.hadoop.HBaseMapReduceIndexJobsUtils;
import org.janusgraph.hadoop.MapReduceIndexManagement;
import org.janusgraph.util.system.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MapReduceTest extends AbstractKGgraphTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceTest.class);
    @Test
    public void repair(){
        String dataPath=this.getClass().getResource("/trsgraph-hbase-es-244_es7.properties").getFile();
        try {
            HBaseMapReduceIndexJobsUtils.repair(dataPath,"document_news","");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void repair2(){
        String dataPath=this.getClass().getResource("/trsgraph-hbase-es-244_es7.properties").getFile();
        try {
            Properties properties = new Properties();
            FileInputStream fis = null;
            try {
                Configuration hadoopBaseConf=new Configuration();
                //hadoopBaseConf.set("janusgraphmr.scanjob.conf.job.index.batchSize","1000");
                hadoopBaseConf.addResource(new Path("C:\\Users\\ldp\\Downloads\\yarn-conf\\mapred-site.xml"));
                //hadoopBaseConf.addResource(new FileInputStream(new File("C:\\Users\\ldp\\Downloads\\yarn-conf\\mapred-site.xml")));
                fis = new FileInputStream(dataPath);
                properties.load(fis);
                ScanMetrics object_qq = HBaseMapReduceIndexJobsUtils.repair(properties, "document_news", "",hadoopBaseConf);
            } finally {
                IOUtils.closeQuietly(fis);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 只能移除组合索引
     */
    @Test
    public void remove(){
        String dataPath=this.getClass().getResource("/trsgraph-hbase-es-244_es7.properties").getFile();
        try {
            HBaseMapReduceIndexJobsUtils.remove(dataPath,"object_qq","");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void repair1(){
        JanusGraphManagement mgmt = getJanusGraph().openManagement();
        try {
            MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
            mr.updateIndex(mgmt.getGraphIndex("object_qq"), SchemaAction.REINDEX).get();
            mgmt.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }


}

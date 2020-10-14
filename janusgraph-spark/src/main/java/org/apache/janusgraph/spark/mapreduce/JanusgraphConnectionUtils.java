package org.apache.janusgraph.spark.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: ldp
 * @time: 2020/10/14 9:42
 * @jira:
 */
@Slf4j
public class JanusgraphConnectionUtils {
    private volatile static JanusgraphConnectionUtils singleTon;
    private Map<String, JanusGraph> janusGraphMap;
    private JanusgraphConnectionUtils(){
        this.janusGraphMap=new HashMap<>();
    }
    public static JanusgraphConnectionUtils createInstance(){
        if(singleTon==null){
            synchronized(JanusgraphConnectionUtils.class){
                if(singleTon==null){
                    singleTon=new JanusgraphConnectionUtils();
                }
            }
        }
        return  singleTon;
    }

    public synchronized JanusGraph janusGraphConnection(final Configuration graphComputerConfiguration){
        String graphName = graphComputerConfiguration.getString("storage.hbase.table");
        if(!janusGraphMap.containsKey(graphName)){
            JanusGraph janusGraph = JanusGraphFactory.open(graphComputerConfiguration);
            log.info(String.format("thread %s,create janusgraph connection %s.....",Thread.currentThread().getName(),graphName));
            janusGraphMap.put(graphName,janusGraph);
        }
        JanusGraph janusGraph= janusGraphMap.get(graphName);
        log.info(String.format("thread %s,get janusgraph connection %s",Thread.currentThread().getName(),janusGraph.getUniqueInstanceId()));
        return janusGraph;
    }

}

package org.apache.janusgraph.spark.mapreduce;

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

    public JanusGraph janusGraphConnection(final Configuration graphComputerConfiguration){
        String graphName = graphComputerConfiguration.getString("storage.hbase.table");
        if(!janusGraphMap.containsKey(graphName)){
            JanusGraph janusGraph = JanusGraphFactory.open(graphComputerConfiguration);
            janusGraphMap.put(graphName,janusGraph);
        }
        return janusGraphMap.get(graphName);
    }

}

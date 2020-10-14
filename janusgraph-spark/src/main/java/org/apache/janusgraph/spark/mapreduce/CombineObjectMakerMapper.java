package org.apache.janusgraph.spark.mapreduce;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author: ldp
 * @time: 2020/9/15 10:30
 * @jira: 给对象打上被合并标记
 */
public class CombineObjectMakerMapper extends AbstractCombineObjectMapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(CombineObjectMakerMapper.class);
    //如果一个对象有了“merge_to”属性则表示这个对象是被合并对象，这个对象会被合并到“merge_to”属性值对应的对象上。
    public static final String COMBINE_MAKER_MEMORY_KEY = "janusgraph.combineMaker.memoryKey";
    public static final String MEMORYKEY = "CombineObjectMaker";

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(COMBINE_MAKER_MEMORY_KEY, this.MEMORYKEY);
    }

    @Override
    public void combine(String key, Iterator<Long> values, ReduceEmitter<String, Long> emitter) {
        if(values!=null){
            ArrayList<Long> vids = Lists.newArrayList(values);
            if(vids.size()>0) {
                if (vids.size() > 1) {
                    Iterator<Vertex> vertices = graph.vertices(vids.toArray());
                    ArrayList<Vertex> vertexArrayList = Lists.newArrayList(vertices);
                    Long toMergeId=this.makerVertex(vertexArrayList);
                    if(toMergeId!=null) {
                        emitter.emit(key, toMergeId);
                    }
                    graph.tx().commit();
                } else {
                    Long vid = vids.get(0);
                    emitter.emit(key, vid);
                }
            }
        }
    }

    @Override
    public void reduce(String key, Iterator<Long> values, ReduceEmitter<String, Long> emitter) {
        if(values!=null){
            ArrayList<Long> vids = Lists.newArrayList(values);
            if(vids.size()>1){
                Iterator<Vertex> vertices = graph.vertices(vids.toArray());
                ArrayList<Vertex> vertexArrayList = Lists.newArrayList(vertices);
                Long toVertexId = this.makerVertex(vertexArrayList);
                if(toVertexId!=null) {
                    emitter.emit(key, toVertexId);
                }
                graph.tx().commit();
            }
        }
    }

    /**
     * 被合并对象打上被合并标志
     * @param vertices 被合并对象
     * @return 合并对象
     */
    private Long makerVertex(List<Vertex> vertices){
        if(vertices!=null&&vertices.size()>1){
            Vertex toVertex = vertices.get(0);
            Long toVertexId=(Long)toVertex.id();
            VertexProperty<Object> mergeProperty = toVertex.property(MAKER_PROPERTY_NAME);
            if(mergeProperty.isPresent()){
                mergeProperty.remove();
            }
            for(int i=1;i<vertices.size();i++){
                Vertex fromVertex = vertices.get(i);
                fromVertex.property(MAKER_PROPERTY_NAME,toVertexId);
            }
            return toVertexId;
        }
        return null;
    }

    @Override
    public Map<String, Long> generateFinalResult(Iterator<KeyValue<String, Long>> keyValues) {
        Map<String,Long> result = new HashMap<>();
       /* for (; keyValues.hasNext(); ) {
            KeyValue<String, Long> r =  keyValues.next();
            result.put(r.getKey(),r.getValue());
        }*/
        return result;
    }

    @Override
    public String getMemoryKey() {
        return MEMORYKEY;
    }

}

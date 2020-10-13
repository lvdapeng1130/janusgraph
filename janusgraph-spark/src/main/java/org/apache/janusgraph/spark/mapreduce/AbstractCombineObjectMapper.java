package org.apache.janusgraph.spark.mapreduce;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.janusgraph.spark.computer.SparkConstants;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: ldp
 * @time: 2020/9/25 13:27
 * @jira: 处理对象合并job的超类
 */
public abstract class AbstractCombineObjectMapper extends StaticMapReduce<String,Long,String,Long, Map<String,Long>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCombineObjectMapper.class);
    public static final String MAKER_PROPERTY_NAME="merge_to";
    protected Graph graph;
    protected Configuration configuration;
    private Set<String> combineObjectTypes;
    private List<String> combinePropertyTypeKeys;
    //参与分组的key是否加上对象类型
    private boolean thinkOverObjectType=true;
    protected StandardJanusGraphTx threadedTx;
    protected int batch=0;
    @Override
    public void loadState(Graph graph, Configuration configuration) {
        this.graph=graph;
        this.configuration=configuration;
        this.thinkOverObjectType=this.configuration.getBoolean(SparkConstants.COMBINE_CONDITION_THINKOVER_OBJECT_TYPE,true);
        String combineObjectType=this.configuration.getString(SparkConstants.COMBINE_CONDITION_OBJECT_TYPE,null);
        if(StringUtils.isNotBlank(combineObjectType)){
            this.combineObjectTypes=Arrays.stream(combineObjectType.split(",")).filter(key-> StringUtils.isNotBlank(key)).map(key->key.trim()).collect(Collectors.toSet());
        }
        this.combinePropertyTypeKeys= Arrays.stream(this.configuration.getString(SparkConstants.COMBINE_CONDITION_PROPERTY_TYPE,"tid")
            .split(",")).filter(key-> StringUtils.isNotBlank(key)).map(key->key.trim()).collect(Collectors.toList());
    }

    @Override
    public boolean doStage(Stage stage) {
        return true;
    }

    @Override
    public void workerEnd(Stage stage) {

        if(this.threadedTx!=null&&this.threadedTx.isOpen()){
            this.threadedTx.commit();
            this.threadedTx.close();
        }
        if(graph instanceof StandardJanusGraph) {
            StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
            if(janusGraph.isOpen()) {
                janusGraph.close();
                LOGGER.info(String.format("%s的workerEnd当前uuid->%s,state->%s关闭Graph实例", this.getMemoryKey(),janusGraph.getUniqueInstanceId(), stage.name()));
            }
        }
    }

    protected void submit(StandardJanusGraph janusGraph){
        if(batch>=1000) {
            long begin = System.currentTimeMillis();
            this.threadedTx.commit();
            long end = System.currentTimeMillis();
            LOGGER.info(String.format("%s的submit当前uuid->%s,提交事务用时%s", this.getMemoryKey(),janusGraph.getUniqueInstanceId(),(end-begin)));
            if (this.threadedTx.isOpen()) {
                this.threadedTx.close();
            }
            batch=0;
        }else{
            batch++;
        }
    }

    @Override
    public void map(Vertex vertex, MapEmitter<String, Long> emitter) {
        if(this.isCombine(vertex)) {
            String combineKey = this.combineKey(vertex);
            if(StringUtils.isNotBlank(combineKey)){
                emitter.emit(combineKey,(Long) vertex.id());
            }
        }
    }


    protected boolean isCombine(Vertex vertex){
        //对对象类型没有限制时所有类型都参加合并业务逻辑
        if(combineObjectTypes==null||combineObjectTypes.size()==0){
            return true;
        }else {
            String label = vertex.label();
            //只有满足给定的类型才允许参与合并业务
            if (combineObjectTypes.contains(label)) {
                return true;
            }else{
                return false;
            }
        }
    }

    protected String combineKey(Vertex vertex){
        StringBuilder keys = new StringBuilder();
        if(this.isThinkOverObjectType()){
            String label = vertex.label();
            keys.append(label).append("-");
        }
        for(String combinePropertyKey:combinePropertyTypeKeys){
            VertexProperty<Object> qqqun_num = vertex.property(combinePropertyKey);
            if(!qqqun_num.isPresent()){
                return null;
            }else{
                Object propertyObject = qqqun_num.value();
                String propertyValue = propertyObject==null?"null":propertyObject.toString();
                keys.append(propertyValue).append("-");
            }
        }
        String propertyValueKeys = keys.toString();
        String propertyValueKey = propertyValueKeys.substring(0, propertyValueKeys.length() - 1);
        return propertyValueKey;
    }

    public Set<String> getCombineObjectTypes() {
        return combineObjectTypes;
    }

    public void setCombineObjectTypes(Set<String> combineObjectTypes) {
        this.combineObjectTypes = combineObjectTypes;
    }

    public List<String> getCombinePropertyTypeKeys() {
        return combinePropertyTypeKeys;
    }

    public void setCombinePropertyTypeKeys(List<String> combinePropertyTypeKeys) {
        this.combinePropertyTypeKeys = combinePropertyTypeKeys;
    }

    public boolean isThinkOverObjectType() {
        return thinkOverObjectType;
    }

    public void setThinkOverObjectType(boolean thinkOverObjectType) {
        this.thinkOverObjectType = thinkOverObjectType;
    }
}

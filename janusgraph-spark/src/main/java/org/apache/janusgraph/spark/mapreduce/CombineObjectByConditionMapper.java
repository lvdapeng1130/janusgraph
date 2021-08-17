package org.apache.janusgraph.spark.mapreduce;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.janusgraph.spark.computer.SparkConstants;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author: ldp
 * @time: 2020/9/15 10:30
 * @jira:
 */
public class CombineObjectByConditionMapper extends AbstractCombineObjectMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(CombineObjectByConditionMapper.class);
    public static final String COMBINE_OBJECT_BY_CONDITION = "CombineObjectByCondition";
    private Set<String> eliminatePropertyTypes;
    private Set<String> eliminateLinkTypes;

    @Override
    public void loadState(Graph graph, Configuration configuration) {
        super.loadState(graph,configuration);
        String eliminatePropertyTypeString=this.configuration.getString(SparkConstants.COMBINE_ELIMINATE_PROPERTY_TYPE,null);
        if(StringUtils.isNotBlank(eliminatePropertyTypeString)){
            this.eliminatePropertyTypes=Arrays.stream(eliminatePropertyTypeString.split(",")).filter(key-> StringUtils.isNotBlank(key)).map(key->key.trim()).collect(Collectors.toSet());
        }else{
            this.eliminatePropertyTypes=Sets.newHashSet();
        }
        String eliminateLinkTypeString=this.configuration.getString(SparkConstants.COMBINE_ELIMINATE_LINK_TYPE,null);
        if(StringUtils.isNotBlank(eliminateLinkTypeString)){
            this.eliminateLinkTypes=Arrays.stream(eliminateLinkTypeString.split(",")).filter(key-> StringUtils.isNotBlank(key)).map(key->key.trim()).collect(Collectors.toSet());
        }else{
            this.eliminateLinkTypes= Sets.newHashSet();
        }
        this.initDefaultEliminatePropertyTypes();
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(COMBINE_OBJECT_BY_CONDITION, COMBINE_OBJECT_BY_CONDITION);
    }
    @Override
    public void combine(String key, Iterator<Long> values, ReduceEmitter<String, Long> emitter) {
        if(values!=null){
            ArrayList<Long> vids = Lists.newArrayList(values);
            if(vids.size()>0) {
                if (vids.size() > 1) {
                    if(graph instanceof StandardJanusGraph) {
                        StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
                        if(this.threadedTx==null||this.threadedTx.isClosed()) {
                            this.threadedTx = (StandardJanusGraphTx) janusGraph.buildTransaction().consistencyChecks(false)
                                .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                        }
                        List<Vertex> vertexList = this.threadedTx.traversal().V(vids.toArray()).toList();
                        Long toMergeId=this.mergeVertex(vertexList);
                        if(toMergeId!=null) {
                            emitter.emit(key, toMergeId);
                        }
                        this.submit(janusGraph,vertexList.size());
                    }
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
                if(graph instanceof StandardJanusGraph) {
                    StandardJanusGraph janusGraph = (StandardJanusGraph) graph;
                    if(this.threadedTx==null||this.threadedTx.isClosed()) {
                        this.threadedTx = (StandardJanusGraphTx) janusGraph.buildTransaction().consistencyChecks(false)
                            .checkInternalVertexExistence(true).checkExternalVertexExistence(true).start();
                    }
                    List<Vertex> vertexList = this.threadedTx.traversal().V(vids.toArray()).toList();
                    this.mergeVertex(vertexList);
                    Long size = Long.parseLong(vertexList.size()+"");
                    emitter.emit(key,size);
                    this.submit(janusGraph,vertexList.size());
                }
            }
        }
            /*GraphTraversalSource g = graph.traversal();
            g.V(vertex.id()).drop().iterate();
            g.tx().commit();*/
    }

    @Override
    public Map<String, Long> generateFinalResult(Iterator<KeyValue<String, Long>> keyValues) {
        Map<String,Long> result = new HashMap<>();
      /*  for (; keyValues.hasNext(); ) {
            KeyValue<String, Long> r =  keyValues.next();
            result.put(r.getKey(),r.getValue());
        }*/
        return result;
    }

    @Override
    public String getMemoryKey() {
        return COMBINE_OBJECT_BY_CONDITION;
    }


    /**
     * 初始化合并时跳过的属性
     */
    private void initDefaultEliminatePropertyTypes(){
        this.eliminatePropertyTypes.add("tid");
        this.eliminatePropertyTypes.add(MAKER_PROPERTY_NAME);
    }

    private Long mergeVertex(List<Vertex> vertices){
        if(vertices!=null){
            Vertex toVertex = this.findToVertex(vertices);
            if(toVertex!=null) {
                if (vertices.size() > 1) {
                    for (int i = 0; i < vertices.size(); i++) {
                        Vertex fromVertex = vertices.get(i);
                        //遍历当前不是被合并对象
                        if(toVertex!=fromVertex) {
                            this.merageVertexProperties(fromVertex, toVertex);
                            this.disposeEdges(toVertex, fromVertex);
                            //删除被合并对象
                            fromVertex.remove();
                        }
                    }
                }
                return (Long) toVertex.id();
            }
        }
        return null;
    }

    private Vertex findToVertex(List<Vertex> vertices){
        if(vertices!=null){
            for(Vertex vertex:vertices){
                VertexProperty<Object> mergeProperty = vertex.property(MAKER_PROPERTY_NAME);
                if(mergeProperty.isPresent()){
                    return vertex;
                }
            }
        }
        return null;
    }

    private void disposeEdges(Vertex toVertex, Vertex fromVertex) {
        long fromVertextId=(Long)fromVertex.id();
        Iterator<Edge> edges = fromVertex.edges(Direction.BOTH);
        while(edges.hasNext()){
            Edge fromEdge = edges.next();
            if(this.isCombineLink(fromEdge.label())) {
                Vertex inVertex = fromEdge.inVertex();
                long inVertextId = (Long) inVertex.id();
                Vertex outVertex = fromEdge.outVertex();
                //被合并对象是入端(In)
                if (fromVertextId == inVertextId) {
                    this.mergeEdge(toVertex, fromEdge, outVertex, true);
                } else {
                    //被合并对象是出端(out)
                    this.mergeEdge(toVertex, fromEdge, inVertex, false);
                }
            }
            //删除原来的关系
            fromEdge.remove();
        }
    }

    private void mergeEdge(Vertex toVertex, Edge fromEdge, Vertex vertex,boolean vertexIsOut) {
        Long mergePropertyValue=obtainMergePropertyValue(vertex);
        Edge newEdge=null;
        //不是被合并对象
        if(mergePropertyValue==null){
            if(vertexIsOut) {
                newEdge = vertex.addEdge(fromEdge.label(), toVertex);
            }else{
                newEdge = toVertex.addEdge(fromEdge.label(), vertex);
            }
            this.merageEdgeProperties(fromEdge,newEdge);
        }else{
            Iterator<Vertex> newVertexIter = toVertex.graph().vertices(mergePropertyValue);
            if(newVertexIter.hasNext()){
                Vertex newVertex = newVertexIter.next();
                if(vertexIsOut) {
                    newEdge = newVertex.addEdge(fromEdge.label(), toVertex);
                }else{
                    newEdge = toVertex.addEdge(fromEdge.label(), newVertex);
                }
                this.merageEdgeProperties(fromEdge,newEdge);
            }
        }
    }

    /**
     * 判断一个对象是否是被合并对象，如果是被合并对象则返回“merge_to”属性的值，否则返回null
     * @param vertex 被判断的对象
     * @return “merge_to”属性的值，返回null则表示对象不是被合并对象
     */
    private Long obtainMergePropertyValue(Vertex vertex) {
        VertexProperty<Object> mergeProperty = vertex.property(MAKER_PROPERTY_NAME);
        if(mergeProperty.isPresent()){
            Object propertyObject = mergeProperty.value();
            Long mergePropertyValue = propertyObject==null?null:(Long)propertyObject;
            return mergePropertyValue;
        }else{
            return null;
        }
    }

    private void merageVertexProperties(Vertex fromVertex,Vertex toVertex){
        Iterator<VertexProperty<Object>> vertexProperties = fromVertex.properties();
        while (vertexProperties.hasNext()){
            VertexProperty<Object> vertexProperty = vertexProperties.next();
            if(vertexProperty.isPresent()){
                String propertyType = vertexProperty.key();
                if(this.isCombineProperty(propertyType)) {
                    VertexProperty<Object> newProperty = toVertex.property(propertyType, vertexProperty.value());
                    Iterator<Property<Object>> properties = vertexProperty.properties();
                    while (properties.hasNext()) {
                        Property<Object> property = properties.next();
                        if (property.isPresent()) {
                            newProperty.property(property.key(), property.value());
                        }
                    }
                }
            }
        }
    }

    private void merageEdgeProperties(Edge fromEdge,Edge toEdge){
        Iterator<Property<Object>> vertexProperties = fromEdge.properties();
        while (vertexProperties.hasNext()){
            Property<Object> edgePropertys = vertexProperties.next();
            if(edgePropertys.isPresent()){
                String propertyType = edgePropertys.key();
                if(this.isCombineProperty(propertyType)) {
                    toEdge.property(propertyType, edgePropertys.value());
                }
            }
        }
    }

    private boolean isCombineProperty(String propertyType){
        if(eliminatePropertyTypes!=null||eliminatePropertyTypes.size()==0)
        {
            return true;
        }
        return !eliminatePropertyTypes.contains(propertyType);
    }

    private boolean isCombineLink(String linkType){
        if(eliminateLinkTypes!=null||eliminateLinkTypes.size()==0)
        {
            return true;
        }
        return !eliminatePropertyTypes.contains(linkType);
    }

}

package org.janusgraph.kydsj;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.janusgraph.spark.structure.io.gryo.GryoRegistrator;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Index;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.util.Constants;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.Note;
import org.janusgraph.util.encoding.LongEncoding;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.DefaultFields;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class IndexRepairJobProcessor implements VoidFunction<Iterator<VertexWritable>> {
    private static ObjectMapper objectMapper=new ObjectMapper();
    protected StandardJanusGraph graph;
    protected ManagementSystem managementSystem;
    protected StandardJanusGraphTx writeTx;
    protected StandardJanusGraphTx searchTx;
    protected Integer batchSize=5000;
    private Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
    private int currentPointer=0;
    private long prevSubmitTime =0;
    private Set<String> vertextLabelSet;
    private Set<String> edgeLabelSet;
    private Map<String, Object> propertieMap;
    private MapConfiguration config;
    private String indexName;

    public IndexRepairJobProcessor(Map<String, Object> propertieMap, String indexName){
        this.propertieMap=propertieMap;
        this.indexName=indexName;
    }
    private void setup(){
        MapConfiguration config = ConfigurationUtil.loadMapConfiguration(propertieMap);
        if (!config.containsKey(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_SERIALIZER)) {
            config.setProperty(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_SERIALIZER, KryoSerializer.class.getCanonicalName());
            if (!config.containsKey(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_KRYO_REGISTRATOR))
                config.setProperty(org.apache.tinkerpop.gremlin.hadoop.Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getCanonicalName());
        }
        this.graph = (StandardJanusGraph) JanusGraphFactory.open(config);
        //this.graph = (StandardJanusGraph)JanusgraphConnectionUtils.createInstance().janusGraphConnection(config);
        this.vertextLabelSet=new HashSet<>();
        this.edgeLabelSet=new HashSet<>();
        if(StringUtils.isNotBlank(indexName)){
            Set<String> indexSet = Arrays.stream(indexName.split(",")).filter(f -> StringUtils.isNotBlank(f)&&!"index_test".equalsIgnoreCase(f)).map(f->f.trim()).collect(Collectors.toSet());
            for(String index:indexSet) {
                if(graph.containsVertexLabel(index)){
                    this.vertextLabelSet.add(index);
                }
                if(graph.containsEdgeLabel(index)) {
                    this.edgeLabelSet.add(index);
                }
            }
        }
        try {
            this.managementSystem = (ManagementSystem)graph.openManagement();
            StandardTransactionBuilder txb = this.graph.buildTransaction();
            txb.setIndexMode(true);
            writeTx = (StandardJanusGraphTx)txb.start();
            StandardTransactionBuilder search = this.graph.buildTransaction();
            search.setIndexMode(true);
            searchTx = (StandardJanusGraphTx)search.start();
            this.prevSubmitTime=System.currentTimeMillis();
        } catch (final Exception e) {
            if (null != managementSystem && managementSystem.isOpen())
                managementSystem.rollback();
            if (writeTx!=null && writeTx.isOpen()) {
                writeTx.rollback();
            }
            if(this.graph!=null&&this.graph.isOpen()){
                this.graph.close();
            }
            throw new JanusGraphException(e.getMessage(), e);
        }
    }
    private void cleanup(){
        try {
            if (documentsPerStore.size() > 0) {
                StringBuilder stringBuilder=new StringBuilder(Thread.currentThread().getName()+"写入");
                for(Map.Entry<String,Map<String, List<IndexEntry>>> entry:documentsPerStore.entrySet()) {
                    stringBuilder.append("[").append(entry.getKey()).append("=").append(entry.getValue().size()).append("]");
                }
                BackendTransaction mutator = writeTx.getTxHandle();
                long begin = System.currentTimeMillis();
                mutator.getIndexTransaction("search").restore(documentsPerStore);
                long end=System.currentTimeMillis();
                long freeMemory = Runtime.getRuntime().freeMemory();
                long totalMemory = Runtime.getRuntime().totalMemory();
                long maxMemory = Runtime.getRuntime().maxMemory();
                long usable = maxMemory - totalMemory + freeMemory;
                log.info(String.format("%s,向es写数据用时%s,共%s条,处理该批次数据用时%s,freeMemory=%sM,totalMemory=%sM,maxMemory=%sM,usable=%sM",stringBuilder.toString(),
                    (end-begin),currentPointer,(end-prevSubmitTime),freeMemory/1024/1024,totalMemory/1024/1024,maxMemory/1024/1024,usable/1024/1024));
                this.prevSubmitTime=end;
                documentsPerStore = new HashMap<>();
                currentPointer=0;
            }
            log.info("结束！！！！！！！！！！！！！！！！！！！！！！！！！");
        } catch (Exception e) {
            //throw new JanusGraphException(e.getMessage(), e);
            log.error(e.getMessage(),e);
        } finally {
            this.workerIterationEndOper();
            if(searchTx.isOpen()) {
                searchTx.rollback();
            }
        }
    }
    private void workerIterationEndOper() {
        try {
            if (null != managementSystem && managementSystem.isOpen()) {
                managementSystem.rollback();
            }
            if (writeTx!=null && writeTx.isOpen()) {
                writeTx.commit();
            }
            if(this.graph!=null&&this.graph.isOpen()){
                this.graph.close();
            }
        } catch (RuntimeException e) {
            log.error("Transaction commit threw runtime exception:", e);
        }
    }
    @Override
    public void call(Iterator<VertexWritable> vertexIterator) throws Exception {
        this.setup();
        while (vertexIterator.hasNext()){
            VertexWritable vertexWritable = vertexIterator.next();
            StarGraph.StarVertex vertex = vertexWritable.get();
            String label = vertex.label();
            //1、处理对象在elasticsearch中的索引
            if(this.includeVertextLabel(label)) {
                Index index = managementSystem.getGraphIndex(label);
                if (index != null) {
                    IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
                    assert indexType != null;
                    if (indexType.isMixedIndex()) {
                        if (this.reindexElement(vertex, (MixedIndexType) indexType, documentsPerStore)) {
                            currentPointer++;
                        }
                    }
                }
            }
            //2、处理对象的关系在elasticsearch中的索引
            if(this.isDisposeEdge()) {
                Iterator<Edge> edgeIterator = vertex.edges(Direction.OUT);
                while (edgeIterator.hasNext()) {
                    Edge edge = edgeIterator.next();
                    String edgeLabel = edge.label();
                    if (this.includeEdgeLabel(edgeLabel)) {
                        Index edgeIndex = managementSystem.getGraphIndex(edgeLabel);
                        if (edgeIndex != null) {
                            IndexType indexType = managementSystem.getSchemaVertex(edgeIndex).asIndexType();
                            assert indexType != null;
                            if (indexType.isMixedIndex()) {
                                if (this.reindexEdgeElement(edge, (MixedIndexType) indexType, documentsPerStore)) {
                                    currentPointer++;
                                    this.submitEx();
                                }
                            }
                        }
                    }
                }
            }
            //批量提交数据向es写索引
            this.submitEx();
        }
        this.cleanup();
    }

    private void submitEx() {
        int size=currentPointer;
        long freeMemory = Runtime.getRuntime().freeMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usable = maxMemory - totalMemory + freeMemory;
        long usablePercent=getPercent(usable,maxMemory);
        if(currentPointer>=batchSize||usablePercent<=25){
            StringBuilder stringBuilder=new StringBuilder(Thread.currentThread().getName()+"写入");
            for(Map.Entry<String,Map<String, List<IndexEntry>>> entry:documentsPerStore.entrySet()) {
                stringBuilder.append("[").append(entry.getKey()).append("=").append(entry.getValue().size()).append("]");
            }
            long begin = System.currentTimeMillis();
            this.submit();
            long end=System.currentTimeMillis();
            log.info(String.format("%s,向es写数据用时%s,共%s条,处理该批次数据用时%s,freeMemory=%sM,totalMemory=%sM,maxMemory=%sM,usable=%sM,usablePercent=%s",stringBuilder.toString(),
                (end-begin),size,(end- prevSubmitTime),freeMemory/1024/1024,totalMemory/1024/1024,maxMemory/1024/1024,usable/1024/1024,usablePercent));
            this.prevSubmitTime =end;
        }
    }

    private void submit(){
        try {
            if (documentsPerStore.size() > 0) {
                BackendTransaction mutator = writeTx.getTxHandle();
                mutator.getIndexTransaction("search").restore(documentsPerStore);
                documentsPerStore = new HashMap<>();
                currentPointer=0;
            }
        } catch (Exception e) {
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    public boolean reindexElement(StarGraph.StarVertex vertex, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        final List<IndexEntry> entries = Lists.newArrayList();
        Iterator<VertexProperty<Object>> properties = vertex.properties();
        while(properties.hasNext()){
            VertexProperty<Object> vertexProperty = properties.next();
            String label = vertexProperty.label();
            Object value = vertexProperty.value();
            if(value!=null) {
                if(label.equals(BaseKey.VertexAttachment.name())){
                    //附件正文抽到elasticsearch
                    MediaData mediaData=(MediaData)value;
                    IndexEntry indexEntry = getIndexEntry(mediaData);
                    if(indexEntry!=null) {
                        entries.add(indexEntry);
                    }
                }else if(label.equals(BaseKey.VertexNote.name())){
                    //注释
                    Note note=(Note)value;
                    IndexEntry indexEntry = getIndexEntry(note);
                    if(indexEntry!=null) {
                        entries.add(indexEntry);
                    }
                }else{
                    IndexEntry indexEntry = new IndexEntry(label, value);
                    Iterator<Property<Object>> properties1 = vertexProperty.properties();
                    while(properties1.hasNext()){
                        Property<Object> property = properties1.next();
                        if(property.isPresent()){
                            String name = property.key();
                            Object propertyValue = property.value();
                            if(name.equals("startDate")) {
                                indexEntry.setStartDate((Date)propertyValue);
                            }else if(name.equals("endDate")){
                                indexEntry.setEndDate((Date)propertyValue);
                            }else if(name.equals("geo")){
                                Geoshape geoshape = (Geoshape)propertyValue;
                                if(geoshape!=null&&geoshape.getPoint()!=null){
                                    Geoshape.Point point = geoshape.getPoint();
                                    indexEntry.setGeo(new double[]{point.getLongitude(),point.getLatitude()});
                                }
                            }else if(name.equals("role")) {
                                String role = (String)propertyValue;
                                indexEntry.setRole(role);
                            }else if(name.equals("dsr")){
                                String dsr = (String)propertyValue;
                                if(StringUtils.isNotBlank(dsr)) {
                                    indexEntry.setDsr(Sets.newHashSet(dsr));
                                }
                            }
                        }
                    }
                    entries.add(indexEntry);
                }
            }
        }
        if (entries.isEmpty())
            return false;
        getDocuments(documentsPerStore, index).put(element2String(vertex), entries);
        return true;
    }

    private IndexEntry getIndexEntry(Note note) {
        if(note !=null) {
            Map<String, String> map = new HashMap<>();
            map.put("title", note.getNoteTitle());
            map.put("content", note.getNoteData());
            try {
                String json = objectMapper.writeValueAsString(map);
                IndexEntry indexEntry = new IndexEntry(DefaultFields.NOTESET.getName(), json);
                Set<String> dsr = note.getDsr();
                if (dsr == null) {
                    dsr = new HashSet<>();
                }
                if (StringUtils.isNotBlank(note.getUser())) {
                    dsr.add(note.getUser());
                }
                if (dsr.size() > 0) {
                    indexEntry.setDsr(dsr);
                }
                indexEntry.setRole(note.getId());
                indexEntry.setStartDate(note.getUpdateDate());
                indexEntry.setEndDate(note.getUpdateDate());
                return indexEntry;
            } catch (JsonProcessingException e) {
            }
        }
        return null;
    }

    private IndexEntry getIndexEntry(MediaData mediaData) {
        if(mediaData!=null&&StringUtils.isNotBlank(mediaData.getText())&& Constants.LINK_SIMPLE.equalsIgnoreCase(mediaData.getLinkType())) {
            IndexEntry indexEntry = new IndexEntry(DefaultFields.MEDIASET.getName(), mediaData.getText());
            Set<String> dsr = mediaData.getDsr();
            if (dsr != null && dsr.size() > 0) {
                indexEntry.setDsr(dsr);
            }
            indexEntry.setStartDate(mediaData.getUpdateDate());
            indexEntry.setEndDate(mediaData.getUpdateDate());
            indexEntry.setRole(mediaData.getKey());
            return indexEntry;
        }
        return null;
    }

    public boolean reindexEdgeElement(Edge edge, MixedIndexType index, Map<String,Map<String,List<IndexEntry>>> documentsPerStore) {
        final List<IndexEntry> entries = Lists.newArrayList();
        Set<String> keys=new HashSet<>();
        Iterator<? extends Property<Object>> properties = edge.properties();
        while (properties.hasNext()){
            Property<Object> property = properties.next();
            String propertyName = property.key();
            Object value = property.value();
            if(value!=null&&StringUtils.isNotBlank(propertyName)) {
                IndexEntry indexEntry = new IndexEntry(propertyName, value);
                entries.add(indexEntry);
                keys.add(propertyName);
            }
        }
        Vertex inVertex = edge.inVertex();
        Vertex outVertex = edge.outVertex();
        if(!keys.contains(DefaultFields.LINK_TYPE.getName())) {
            IndexEntry indexLinkTypeEntry = new IndexEntry(DefaultFields.LINK_TYPE.getName(), edge.label());
            entries.add(indexLinkTypeEntry);
        }
        if(!keys.contains(DefaultFields.LEFT_TID.getName())) {
            IndexEntry indexLeftTidEntry = new IndexEntry(DefaultFields.LEFT_TID.getName(), outVertex.id());
            entries.add(indexLeftTidEntry);
        }
        if(!keys.contains(DefaultFields.LEFT_TYPE.getName())) {
            JanusGraphVertex vertex = searchTx.getVertex(outVertex.id().toString());
            String label = vertex.label();
            IndexEntry indexLeftTypeEntry = new IndexEntry(DefaultFields.LEFT_TYPE.getName(), label);
            entries.add(indexLeftTypeEntry);
        }
        if(!keys.contains(DefaultFields.RIGHT_TID.getName())) {
            IndexEntry indexRightTidEntry = new IndexEntry(DefaultFields.RIGHT_TID.getName(), inVertex.id());
            entries.add(indexRightTidEntry);
        }
        if(!keys.contains(DefaultFields.RIGHT_TYPE.getName())) {
            JanusGraphVertex vertex = searchTx.getVertex(inVertex.id().toString());
            String inLabel = vertex.label();
            IndexEntry indexRightTypentry = new IndexEntry(DefaultFields.RIGHT_TYPE.getName(), inLabel);
            entries.add(indexRightTypentry);
        }
        if (entries.isEmpty())
            return false;
        this.getDocuments(documentsPerStore, index).put(element2String(edge.id()), entries);
        return true;
    }

    private Map<String,List<IndexEntry>> getDocuments(Map<String,Map<String,List<IndexEntry>>> documentsPerStore, MixedIndexType index) {
        return documentsPerStore.computeIfAbsent(index.getStoreName(), k -> Maps.newHashMap());
    }

    private static String element2String(StarGraph.StarVertex vertex) {
        return element2String(vertex.id());
    }

    private static String element2String(Object elementId) {
        Preconditions.checkArgument(elementId instanceof String||elementId instanceof RelationIdentifier);
        if (elementId instanceof String) return longID2Name(elementId.toString());
        else return ((RelationIdentifier) elementId).toString();
    }

    private static String longID2Name(String id) {
        Preconditions.checkArgument(StringUtils.isNotBlank(id));
        return LongEncoding.encode(id);
    }
    /**
     * 当满足下面两种情况的任意一种需处理实体
     * 1、整库重建索引时
     * 2、当指定了实体类型时
     * @return
     */
    public boolean includeVertextLabel(String indexName){
        if(vertextLabelSet.size()==0&&edgeLabelSet.size() == 0) {
            return true;
        }
        return vertextLabelSet.contains(indexName);
    }

    public boolean includeEdgeLabel(String indexName){
        if(vertextLabelSet.size()==0&&edgeLabelSet.size() == 0) {
            return true;
        }
        return this.edgeLabelSet.contains(indexName);
    }

    /**
     * 当满足下面两种情况的任意一种需处理关系
     * 1、整库重建索引时
     * 2、当指定了关系类型时
     * @return
     */
    public boolean isDisposeEdge(){
        if(vertextLabelSet.size()==0&&edgeLabelSet.size() == 0) {
            return true;
        }else if(edgeLabelSet.size() !=0){
            return true;
        }
        return false;
    }

    private long getPercent(long count, long total) {
        if(total==0){
            return 0;
        }
        BigDecimal currentCount = new BigDecimal(count);
        BigDecimal totalCount = new BigDecimal(total);
        BigDecimal divide = currentCount.divide(totalCount,2, BigDecimal.ROUND_HALF_UP);
        return divide.multiply(new BigDecimal(100)).longValue();
    }
}

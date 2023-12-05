// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.olap;

import org.janusgraph.diskstorage.PropertyEntry;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.graphdb.database.idassigner.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PropertyPropertyInfo;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.MediaDataRaw;
import org.janusgraph.kydsj.serialize.Note;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexJobConverter extends AbstractScanJob {

    protected static final SliceQuery VERTEX_EXISTS_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1),BufferUtil.oneBuffer(4)).setLimit(1);

    public static final String GHOST_VERTEX_COUNT = "ghost-vertices";
    /**
     * Number of result sets that got (possibly) truncated due to an applied query limit
     */
    public static final String TRUNCATED_ENTRY_LISTS = "truncated-results";

    protected final VertexScanJob job;

    protected VertexJobConverter(JanusGraph graph, VertexScanJob job) {
        super(graph);
        Preconditions.checkArgument(job!=null);
        this.job = job;
    }

    protected VertexJobConverter(VertexJobConverter copy) {
        super(copy);
        this.job = copy.job.clone();
    }

    public static ScanJob convert(JanusGraph graph, VertexScanJob vertexJob) {
        return new VertexJobConverter(graph,vertexJob);
    }

    public static ScanJob convert(VertexScanJob vertexJob) {
        return new VertexJobConverter(null,vertexJob);
    }

    @Override
    public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
        try {
            open(graphConfig);
            job.workerIterationStart(graph.get(), jobConfig, metrics);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    protected StandardJanusGraphTx startTransaction(StandardJanusGraph graph) {
        return (StandardJanusGraphTx) graph.buildTransaction().setIndexMode(true).readOnlyOLAP().start();
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        job.workerIterationEnd(metrics);
        close();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics, PropertyEntry propertyEntry) {
        String vertexId = getVertexId(key);
        assert entries.get(VERTEX_EXISTS_QUERY)!=null;
        if (isGhostVertex(vertexId, entries.get(VERTEX_EXISTS_QUERY))) {
            metrics.incrementCustom(GHOST_VERTEX_COUNT);
            return;
        }
        JanusGraphVertex vertex = tx.getInternalVertex(vertexId);
        Preconditions.checkArgument(vertex instanceof PreloadedVertex,
                "The bounding transaction is not configured correctly");
        PreloadedVertex v = (PreloadedVertex)vertex;
        v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
        for (Map.Entry<SliceQuery,EntryList> entry : entries.entrySet()) {
            SliceQuery sq = entry.getKey();
            if (sq.equals(VERTEX_EXISTS_QUERY)) continue;
            EntryList entryList = entry.getValue();
            if (entryList.size()>=sq.getLimit()) metrics.incrementCustom(TRUNCATED_ENTRY_LISTS);
            v.addToQueryCache(sq.updateLimit(Query.NO_LIMIT),entryList);
        }
        if(propertyEntry!=null) {
            EntryList propertyProperties = propertyEntry.getPropertyProperties() == null ? EntryArrayList.EMPTY_LIST : EntryArrayList.of(propertyEntry.getPropertyProperties());
            Multimap<String, PropertyPropertyInfo> multiPropertyProperties = this.readPropertyProperties(propertyProperties);
            v.setMultiPropertyProperties(multiPropertyProperties);
            Iterator<MediaData> mediaIterator = this.getMediaIterator(propertyEntry.getMedias());
            Iterator<Note> noteIterator = this.getNoteIterator(propertyEntry.getNotes());
            v.setMediaIterator(mediaIterator);
            v.setNoteIterator(noteIterator);
        }
        job.process(v, metrics);
    }

    public Iterator<MediaData> getMediaIterator(Iterable<Entry> medias){
        if(medias!=null) {
            Serializer serializer = tx.getGraph().getDataSerializer();
            Iterator<MediaData> meidaIterator = StreamSupport.stream(medias.spliterator(), true).filter(entry -> {
                ReadBuffer buffer = entry.asReadBuffer();
                String col = serializer.readObjectNotNull(buffer, String.class);
                if (col.startsWith(MediaDataRaw.PREFIX_COL)) {
                    return false;
                }
                return true;
            }).map(entry -> {
                ReadBuffer buffer = entry.asReadBuffer();
                String col = serializer.readObjectNotNull(buffer, String.class);
                MediaData mediaData = serializer.readObjectNotNull(buffer, MediaData.class);
                return mediaData;
            }).iterator();
            return meidaIterator;
        }
        return null;
    }

    public Iterator<Note> getNoteIterator(Iterable<Entry> notes){
        if(notes!=null) {
            Serializer serializer = tx.getGraph().getDataSerializer();
            Iterator<Note> noteIterator = StreamSupport.stream(notes.spliterator(), true).map(entry -> {
                ReadBuffer buffer = entry.asReadBuffer();
                String col = serializer.readObjectNotNull(buffer, String.class);
                Note note = serializer.readObjectNotNull(buffer, Note.class);
                return note;
            }).iterator();
            return noteIterator;
        }
        return null;
    }

    /**
     * 列名=》属性类型id+md5(属性值).substring(8,16)+属性的属性类型id+属性的属性值
     * 列值=》属性值的ID
     * @param propertyProperties
     * @return
     */
    private Multimap<String,PropertyPropertyInfo> readPropertyProperties(EntryList propertyProperties){
        Multimap<String,PropertyPropertyInfo> multimap= ArrayListMultimap.create();
        if(propertyProperties!=null){
            Serializer serializer = tx.getGraph().getDataSerializer();
            Iterator<Entry> iterator = propertyProperties.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                ReadBuffer buffer = entry.asReadBuffer();
                String propertyTypeId = serializer.readObjectNotNull(buffer, String.class);
                String propertyValue_md5 = serializer.readObjectNotNull(buffer, String.class);
                String propertyPropertyKeyId = serializer.readObjectNotNull(buffer, String.class);
                RelationType relationType = tx.getExistingRelationType(propertyPropertyKeyId);
                PropertyKey key = (PropertyKey) relationType;
                Object propertyPropertyValue = this.readPropertyValue(serializer,buffer, key);
                String propertyId = serializer.readObjectNotNull(buffer, String.class);
                PropertyPropertyInfo propertyPropertyInfo = new PropertyPropertyInfo(propertyTypeId, propertyValue_md5,
                    propertyPropertyKeyId,propertyPropertyValue,propertyId,key);
                multimap.put(propertyTypeId+propertyValue_md5,propertyPropertyInfo);
            }
        }
        return multimap;
    }

    private Object readPropertyValue(Serializer serializer,ReadBuffer read, PropertyKey key) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            return serializer.readClassAndObject(read);
        } else {
            return serializer.readObject(read, key.dataType());
        }
    }

    protected boolean isGhostVertex(String vertexId, EntryList firstEntries) {
        if (idManager.isPartitionedVertex(vertexId) && !idManager.isCanonicalVertexId(vertexId)) return false;

        RelationCache relCache = tx.getEdgeSerializer().parseRelation(
                firstEntries.get(0),true,tx);
        return !relCache.typeId.equals(BaseKey.VertexExists.longId());
    }

    @Override
    public List<SliceQuery> getQueries() {
        try {
            QueryContainer qc = new QueryContainer(tx);
            job.getQueries(qc);

            List<SliceQuery> slices = new ArrayList<>();
            slices.add(VERTEX_EXISTS_QUERY);
            slices.addAll(qc.getSliceQueries());
            return slices;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        return buffer -> !IDManager.VertexIDType.Invisible.is(getVertexId(buffer));
    }

    @Override
    public VertexJobConverter clone() {
        return new VertexJobConverter(this);
    }

    protected String getVertexId(StaticBuffer key) {
        return idManager.getKeyID(key);
    }

    public static class GraphProvider {

        private StandardJanusGraph graph=null;
        private boolean provided=false;

        public void setGraph(JanusGraph graph) {
            Preconditions.checkArgument(graph!=null && graph.isOpen(),"Need to provide open graph");
            this.graph = (StandardJanusGraph)graph;
            provided = true;
        }

        public void initializeGraph(Configuration config) {
            if (!provided) {
                this.graph = (StandardJanusGraph) JanusGraphFactory.open((BasicConfiguration) config);
            }
        }

        public void close() {
            if (!provided && null != graph && graph.isOpen()) {
                graph.close();
                graph=null;
            }
        }

        public boolean isProvided() {
            return provided;
        }

        public final StandardJanusGraph get() {
            Preconditions.checkNotNull(graph);
            return graph;
        }

    }

}

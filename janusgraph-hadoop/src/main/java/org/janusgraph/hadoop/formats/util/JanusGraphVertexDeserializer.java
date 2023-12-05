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

package org.janusgraph.hadoop.formats.util;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PropertyEntry;
import org.janusgraph.diskstorage.PropertyPropertyInfo;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.util.MD5Util;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetup;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.MediaDataRaw;
import org.janusgraph.kydsj.serialize.Note;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.StreamSupport;

public class JanusGraphVertexDeserializer implements AutoCloseable {

    private final JanusGraphHadoopSetup setup;
    private final TypeInspector typeManager;
    private final SystemTypeInspector systemTypes;
    private final IDManager idManager;
    private final Serializer serializer;

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphVertexDeserializer.class);
    protected static final SliceQuery VERTEX_EXISTS_QUERY = (new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(4))).setLimit(1);
    private final SliceQuery initialQuery;
    private final SliceQuery labelQuery;
    private final SliceQuery propertyQuery;
    private final SliceQuery edgeQuery;
    private final List<SliceQuery> subsequentQueries;
    public JanusGraphVertexDeserializer(final JanusGraphHadoopSetup setup) {
        this.setup = setup;
        this.typeManager = setup.getTypeInspector();
        this.systemTypes = setup.getSystemTypeInspector();
        this.idManager = setup.getIDManager();
        this.serializer=setup.getDataSerializer();
        List<SliceQuery> sliceQueries = getQueries();
        this.initialQuery=sliceQueries.get(0);
        this.subsequentQueries=new ArrayList<>(sliceQueries.subList(1,sliceQueries.size()));
        this.labelQuery=this.getLabelSliceQuery();
        this.edgeQuery=this.getEdgeSliceQuery();
        this.propertyQuery = setup.getEdgeSerializer().getQuery(RelationCategory.PROPERTY, false);
    }

    public SliceQuery getLabelSliceQuery(){
        QueryContainer qc = new QueryContainer((StandardJanusGraphTx)typeManager);
        qc.addQuery().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).edges();
        List<SliceQuery> sliceQueries1 = qc.getSliceQueries();
        return sliceQueries1.get(0);
    }

    public SliceQuery getEdgeSliceQuery(){
        QueryContainer qc = new QueryContainer((StandardJanusGraphTx)typeManager);
        qc.addQuery().direction(Direction.BOTH).edges();
        List<SliceQuery> sliceQueries = qc.getSliceQueries();
        return sliceQueries.get(0);
    }

    public List<SliceQuery> getQueries() {
        try {
            QueryContainer qc = new QueryContainer((StandardJanusGraphTx)typeManager);
            qc.addQuery().properties();
            qc.addQuery().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).edges();
            qc.addQuery().direction(Direction.BOTH).edges();
            List<SliceQuery> slices = new ArrayList<>();
            slices.add(VERTEX_EXISTS_QUERY);
            slices.addAll(qc.getSliceQueries());
            return slices;
        } catch (Throwable e) {
            throw e;
        }
    }

    private EntryList findEntriesMatchingQuery(SliceQuery query, EntryList sortedEntries) {

        int lowestStartMatch = sortedEntries.size(); // Inclusive
        int highestEndMatch = -1; // Inclusive

        final StaticBuffer queryStart = query.getSliceStart();
        final StaticBuffer queryEnd = query.getSliceEnd();

        // Find the lowest matchStart s.t. query.getSliceStart <= sortedEntries.get(matchStart)

        int low = 0;
        int high = sortedEntries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Entry midVal = sortedEntries.get(mid);

            int cmpStart = queryStart.compareTo(midVal.getColumn());

            if (0 < cmpStart) {
                // query lower bound exceeds entry (no match)
                if (lowestStartMatch == mid + 1) {
                    // lowestStartMatch located
                    break;
                }
                // Move to higher list index
                low = mid + 1;
            } else /* (0 >= cmpStart) */ {
                // entry equals or exceeds query lower bound (match, but not necessarily lowest match)
                if (mid < lowestStartMatch) {
                    lowestStartMatch = mid;
                }
                // Move to a lower list index
                high = mid - 1;
            }
        }

        // If lowestStartMatch is beyond the end of our list parameter, there cannot possibly be any matches,
        // so we can bypass the highestEndMatch search and just return an empty result.
        if (sortedEntries.size() == lowestStartMatch) {
            return EntryList.EMPTY_LIST;
        }

        // Find the highest matchEnd s.t. sortedEntries.get(matchEnd) < query.getSliceEnd

        low = 0;
        high = sortedEntries.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Entry midVal = sortedEntries.get(mid);

            int cmpEnd = queryEnd.compareTo(midVal.getColumn());

            if (0 < cmpEnd) {
                // query upper bound exceeds entry (match, not necessarily highest)
                if (mid > highestEndMatch) {
                    highestEndMatch = mid;
                }
                // Move to higher list index
                low = mid + 1;
            } else /* (0 >= cmpEnd) */ {
                // entry equals or exceeds query upper bound (no match)
                if (highestEndMatch == mid - 1) {
                    // highestEndMatch located
                    break;
                }
                // Move to a lower list index
                high = mid - 1;
            }
        }

        if (0 <= highestEndMatch - lowestStartMatch) {
            // Return sublist between indices (inclusive at both indices)
            int endIndex = highestEndMatch + 1; // This will be passed into subList, which interprets it exclusively
            if (query.hasLimit()) {
                endIndex = Math.min(endIndex, query.getLimit() + lowestStartMatch);
            }
            // TODO avoid unnecessary copy here
            return EntryArrayList.of(sortedEntries.subList(lowestStartMatch, endIndex /* exclusive */));
        } else {
            return EntryList.EMPTY_LIST;
        }
    }

    public Iterator<MediaData> getMediaIterator(Iterable<Entry> medias){
        if(medias!=null) {
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
            Iterator<Entry> iterator = propertyProperties.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                ReadBuffer buffer = entry.asReadBuffer();
                String propertyTypeId = serializer.readObjectNotNull(buffer, String.class);
                String propertyValue_md5 = serializer.readObjectNotNull(buffer, String.class);
                String propertyPropertyKeyId = serializer.readObjectNotNull(buffer, String.class);
                RelationType relationType = typeManager.getExistingRelationType(propertyPropertyKeyId);
                PropertyKey key = (PropertyKey) relationType;
                Object propertyPropertyValue = readPropertyValue(serializer,buffer, key);
                String propertyId = serializer.readObjectNotNull(buffer, String.class);
                PropertyPropertyInfo propertyPropertyInfo = new PropertyPropertyInfo(propertyTypeId, propertyValue_md5,
                    propertyPropertyKeyId,propertyPropertyValue,propertyId,key);
                multimap.put(propertyTypeId+propertyValue_md5,propertyPropertyInfo);
            }
        }
        return multimap;
    }

    protected boolean isGhostVertex(IDManager idManager,StandardJanusGraphTx tx,String vertexId, EntryList firstEntries) {
        if (idManager.isPartitionedVertex(vertexId) && !idManager.isCanonicalVertexId(vertexId)) return false;

        RelationCache relCache = tx.getEdgeSerializer().parseRelation(
            firstEntries.get(0),true,tx);
        return !relCache.typeId.equals(BaseKey.VertexExists.longId());
    }

    private static Object readPropertyValue(Serializer serializer,ReadBuffer read, PropertyKey key) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            return serializer.readClassAndObject(read);
        } else {
            return serializer.readObject(read, key.dataType());
        }
    }

    private boolean edgeExists(Vertex vertex, RelationType type, RelationCache possibleDuplicate) {
        Iterator<Edge> it = vertex.edges(possibleDuplicate.direction, type.name());

        while (it.hasNext()) {
            Edge edge = it.next();

            if (edge.id().equals(possibleDuplicate.relationId)) {
                return true;
            }
        }

        return false;
    }

    public StarGraph.StarVertex readStarVertex(final StaticBuffer key, PropertyEntry propertyEntry) {
        // Convert key to a vertex ID
        final String vertexId = idManager.getKeyID(key);
        Preconditions.checkArgument(StringUtils.isNotBlank(vertexId));

        // Partitioned vertex handling
        if (idManager.isPartitionedVertex(vertexId)) {
            Preconditions.checkState(setup.getFilterPartitionedVertices(),
                "Read partitioned vertex (ID=%s), but partitioned vertex filtering is disabled.", vertexId);
            log.debug("Skipping partitioned vertex with ID {}", vertexId);
            return null;
        }
        EntryArrayList al = EntryArrayList.of(propertyEntry.getProperties());
        EntryList initialQueryMatches = findEntriesMatchingQuery(initialQuery, al);
        if (0 == initialQueryMatches.size()) {
            return null;
        }
        Map<SliceQuery, EntryList> entries = new HashMap<>();
        entries.put(initialQuery, initialQueryMatches);
        for (SliceQuery sq : subsequentQueries) {
            entries.put(sq, findEntriesMatchingQuery(sq, al));
        }
        StandardJanusGraphTx tx = (StandardJanusGraphTx) typeManager;
        if (isGhostVertex(idManager,tx,vertexId, entries.get(VERTEX_EXISTS_QUERY))) {
            return null;
        }
        Multimap<String, PropertyPropertyInfo> multiPropertyProperties=null;
        if(propertyEntry!=null) {
            EntryList propertyProperties = propertyEntry.getPropertyProperties() == null ? EntryArrayList.EMPTY_LIST : EntryArrayList.of(propertyEntry.getPropertyProperties());
            multiPropertyProperties = readPropertyProperties(propertyProperties);
        }
        EntryList labelEntryList = entries.get(this.labelQuery);
        RelationReader relationReader = setup.getRelationReader();
        final RelationCache labelrRelation = relationReader.parseRelation(labelEntryList.get(0), false, typeManager);
        String vertexLabelId = labelrRelation.getOtherVertexId();
        VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
        String label = vl.name();
        //不满足指定的对象类型
        if(!setup.vertexLabelFilter(label)){
            return null;
        }
        final StarGraph starGraph = StarGraph.open();
        final StarGraph.StarVertex starVertex = (StarGraph.StarVertex) starGraph.addVertex(T.id, vertexId, T.label, label);
        EntryList propertiesEntryList = entries.get(this.propertyQuery);
        //属性
        for (final Entry data : propertiesEntryList) {
            try {
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types
                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType)type).isInvisibleType()) continue; //Ignore hidden types
                // Decode and create the relation (edge or property)
                if (type.isPropertyKey()) {
                    // Decode property
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    VertexProperty.Cardinality card = getPropertyKeyCardinality(type.name());
                    VertexProperty<Object> vp = starVertex.property(card, type.name(), value, T.id, relation.relationId);
                    // Decode meta properties
                    decodeProperties(relation, vp);
                    if(multiPropertyProperties!=null) {
                        String propertyValueMD5 = MD5Util.getMD8(value);
                        String propertyTypeId = type.longId();
                        Collection<PropertyPropertyInfo> propertyPropertyInfos = multiPropertyProperties.get(propertyTypeId + propertyValueMD5);
                        if (propertyPropertyInfos != null) {
                            for (PropertyPropertyInfo propertyPropertyInfo : propertyPropertyInfos) {
                                Object dsr = propertyPropertyInfo.getPropertyPropertyValue();
                                if (dsr != null) {
                                    String name = propertyPropertyInfo.getKey().name();
                                    vp.property(name, dsr);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //关系
        EntryList edgeEntryList = entries.get(this.edgeQuery);
        for (final Entry data : edgeEntryList) {
            try {
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types
                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType)type).isInvisibleType()) continue; //Ignore hidden types
                // Decode and create the relation (edge or property)
                assert type.isEdgeLabel();
                String edgeLabel = type.name();
                // Partitioned vertex handling
                if (idManager.isPartitionedVertex(relation.getOtherVertexId())) {
                    Preconditions.checkState(setup.getFilterPartitionedVertices(),
                        "Read edge incident on a partitioned vertex, but partitioned vertex filtering is disabled.  " +
                            "Relation ID: %s.  This vertex ID: %s.  Other vertex ID: %s.  Edge label: %s.",
                        relation.relationId, vertexId, relation.getOtherVertexId(), type.name());
                    log.debug("Skipping edge with ID {} incident on partitioned vertex with ID {} (and nonpartitioned vertex with ID {})",
                        relation.relationId, relation.getOtherVertexId(), vertexId);
                    continue;
                }
                // Decode edge
                Edge starEdge;
                // We don't know the label of the other vertex, but one must be provided
                Vertex adjacentVertex=starGraph.addVertex(T.id,relation.getOtherVertexId());
                // skip self-loop edges that were already processed, but from a different direction
                if (starVertex.equals(adjacentVertex) && edgeExists(starVertex, type, relation)) {
                    continue;
                }
                if (relation.direction.equals(Direction.IN)) {
                    RelationIdentifier id= new RelationIdentifier(relation.getOtherVertexId(), relation.typeId, relation.relationId,vertexId);
                    starEdge=adjacentVertex.addEdge(edgeLabel, starVertex,T.id, id);
                } else if (relation.direction.equals(Direction.OUT)) {
                    RelationIdentifier id= new RelationIdentifier(vertexId, relation.typeId, relation.relationId,relation.getOtherVertexId());
                    starEdge =starVertex.addEdge(edgeLabel, adjacentVertex, T.id, id);
                } else {
                    throw new RuntimeException("Direction.BOTH is not supported");
                }
                decodeProperties(relation, starEdge);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //附件和注释
        if(propertyEntry!=null) {
            Iterator<MediaData> mediaIterator = this.getMediaIterator(propertyEntry.getMedias());
            Iterator<Note> noteIterator = this.getNoteIterator(propertyEntry.getNotes());
            //附件
            if(mediaIterator!=null){
                while (mediaIterator.hasNext()){
                    MediaData mediaData = mediaIterator.next();
                    starVertex.property(VertexProperty.Cardinality.list, BaseKey.VertexAttachment.name(), mediaData, T.id, mediaData.id());
                }
            }
            //注释
            if(noteIterator!=null){
                while (noteIterator.hasNext()){
                    Note note = noteIterator.next();
                    starVertex.property(VertexProperty.Cardinality.list, BaseKey.VertexNote.name(), note, T.id, note.getId());
                }
            }
        }
        return starVertex;
    }

    public PreloadedVertex readHadoopVertex(final StaticBuffer key, PropertyEntry propertyEntry) {
        // Convert key to a vertex ID
        final String vertexId = idManager.getKeyID(key);
        Preconditions.checkArgument(StringUtils.isNotBlank(vertexId));

        // Partitioned vertex handling
        if (idManager.isPartitionedVertex(vertexId)) {
            Preconditions.checkState(setup.getFilterPartitionedVertices(),
                "Read partitioned vertex (ID=%s), but partitioned vertex filtering is disabled.", vertexId);
            log.debug("Skipping partitioned vertex with ID {}", vertexId);
            return null;
        }
        EntryArrayList al = EntryArrayList.of(propertyEntry.getProperties());
        EntryList initialQueryMatches = findEntriesMatchingQuery(initialQuery, al);
        if (0 == initialQueryMatches.size()) {
            return null;
        }
        Map<SliceQuery, EntryList> entries = new HashMap<>();
        entries.put(initialQuery, initialQueryMatches);
        for (SliceQuery sq : subsequentQueries) {
            entries.put(sq, findEntriesMatchingQuery(sq, al));
        }
        StandardJanusGraphTx tx = (StandardJanusGraphTx) typeManager;
        if (isGhostVertex(idManager,tx,vertexId, entries.get(VERTEX_EXISTS_QUERY))) {
            return null;
        }
        //JanusGraphVertex vertex = tx.getInternalVertex(vertexId);
        PreloadedVertex v =  new PreloadedVertex(tx, vertexId, ElementLifeCycle.Loaded);
        v.setAccessCheck(PreloadedVertex.OPENSTAR_CHECK);
        for (Map.Entry<SliceQuery,EntryList> entry : entries.entrySet()) {
            SliceQuery sq = entry.getKey();
            if (sq.equals(VERTEX_EXISTS_QUERY)) continue;
            EntryList entryList = entry.getValue();
            v.addToQueryCache(sq.updateLimit(Query.NO_LIMIT),entryList);
        }
        if(propertyEntry!=null) {
            EntryList propertyProperties = propertyEntry.getPropertyProperties() == null ? EntryArrayList.EMPTY_LIST : EntryArrayList.of(propertyEntry.getPropertyProperties());
            Multimap<String, PropertyPropertyInfo> multiPropertyProperties = readPropertyProperties(propertyProperties);
            v.setMultiPropertyProperties(multiPropertyProperties);
            Iterator<MediaData> mediaIterator = this.getMediaIterator(propertyEntry.getMedias());
            Iterator<Note> noteIterator = this.getNoteIterator(propertyEntry.getNotes());
            v.setMediaIterator(mediaIterator);
            v.setNoteIterator(noteIterator);
        }
        return v;
    }

    // Read a single row from the edgestore and create a TinkerVertex corresponding to the row
    // The neighboring vertices are represented by DetachedVertex instances
    public TinkerVertex readHadoopVertex(final StaticBuffer key, Iterable<Entry> entries) {

        // Convert key to a vertex ID
        final String vertexId = idManager.getKeyID(key);
        Preconditions.checkArgument(StringUtils.isNotBlank(vertexId));

        // Partitioned vertex handling
        if (idManager.isPartitionedVertex(vertexId)) {
            Preconditions.checkState(setup.getFilterPartitionedVertices(),
                    "Read partitioned vertex (ID=%s), but partitioned vertex filtering is disabled.", vertexId);
            log.debug("Skipping partitioned vertex with ID {}", vertexId);
            return null;
        }

        // Create TinkerVertex
        TinkerGraph tg = TinkerGraph.open();

        TinkerVertex tv = null;

        // Iterate over edgestore columns to find the vertex's label relation
        for (final Entry data : entries) {
            RelationReader relationReader = setup.getRelationReader();
            final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
            if (systemTypes.isVertexLabelSystemType(relation.typeId)) {
                // Found vertex Label
                String vertexLabelId = relation.getOtherVertexId();
                VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
                // Create TinkerVertex with this label
                tv = getOrCreateVertex(vertexId, vl.name(), tg);
            } else if (systemTypes.isTypeSystemType(relation.typeId)) {
                log.trace("Vertex {} is a system vertex", vertexId);
                return null;
            }
        }

        // Added this following testing
        if (null == tv) {
            tv = getOrCreateVertex(vertexId, null, tg);
        }

        Preconditions.checkNotNull(tv, "Unable to determine vertex label for vertex with ID %s", vertexId);

        // Iterate over and decode edgestore columns (relations) on this vertex
        for (final Entry data : entries) {
            try {
                RelationReader relationReader = setup.getRelationReader();
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);

                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types
                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType)type).isInvisibleType()) continue; //Ignore hidden types

                // Decode and create the relation (edge or property)
                if (type.isPropertyKey()) {
                    // Decode property
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    VertexProperty.Cardinality card = getPropertyKeyCardinality(type.name());
                    VertexProperty<Object> vp = tv.property(card, type.name(), value, T.id, relation.relationId);

                    // Decode meta properties
                    decodeProperties(relation, vp);
                } else {
                    assert type.isEdgeLabel();

                    // Partitioned vertex handling
                    if (idManager.isPartitionedVertex(relation.getOtherVertexId())) {
                        Preconditions.checkState(setup.getFilterPartitionedVertices(),
                                "Read edge incident on a partitioned vertex, but partitioned vertex filtering is disabled.  " +
                                "Relation ID: %s.  This vertex ID: %s.  Other vertex ID: %s.  Edge label: %s.",
                                relation.relationId, vertexId, relation.getOtherVertexId(), type.name());
                        log.debug("Skipping edge with ID {} incident on partitioned vertex with ID {} (and nonpartitioned vertex with ID {})",
                                relation.relationId, relation.getOtherVertexId(), vertexId);
                        continue;
                    }

                    // Decode edge
                    TinkerEdge te;

                    // We don't know the label of the other vertex, but one must be provided
                    TinkerVertex adjacentVertex = getOrCreateVertex(relation.getOtherVertexId(), null, tg);

                    // skip self-loop edges that were already processed, but from a different direction
                    if (tv.equals(adjacentVertex) && edgeExists(tv, type, relation)) {
                        continue;
                    }

                    if (relation.direction.equals(Direction.IN)) {
                        te = (TinkerEdge)adjacentVertex.addEdge(type.name(), tv, T.id, relation.relationId);
                    } else if (relation.direction.equals(Direction.OUT)) {
                        te = (TinkerEdge)tv.addEdge(type.name(), adjacentVertex, T.id, relation.relationId);
                    } else {
                        throw new RuntimeException("Direction.BOTH is not supported");
                    }
                    decodeProperties(relation, te);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return tv;
    }

    private void decodeProperties(final RelationCache relation, final Element element) {
        if (relation.hasProperties()) {
            // Load relation properties
            for (final ObjectObjectCursor<String,Object> next : relation) {
                assert next.value != null;
                RelationType rt = typeManager.getExistingRelationType(next.key);
                if (rt.isPropertyKey()) {
                    element.property(rt.name(), next.value);
                } else {
                    throw new RuntimeException("Metaedges are not supported");
                }
            }
        }
    }

    public TinkerVertex getOrCreateVertex(final String vertexId, final String label, final TinkerGraph tg) {
        TinkerVertex v;

        try {
            v = (TinkerVertex)tg.vertices(vertexId).next();
        } catch (NoSuchElementException e) {
            if (null != label) {
                v = (TinkerVertex) tg.addVertex(T.label, label, T.id, vertexId);
            } else {
                v = (TinkerVertex) tg.addVertex(T.id, vertexId);
            }
        }

        return v;
    }

    private VertexProperty.Cardinality getPropertyKeyCardinality(String name) {
        RelationType rt = typeManager.getRelationType(name);
        if (null == rt || !rt.isPropertyKey())
            return VertexProperty.Cardinality.single;
        PropertyKey pk = typeManager.getExistingPropertyKey(rt.longId());
        switch (pk.cardinality()) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new IllegalStateException("Unknown cardinality " + pk.cardinality());
        }
    }

    public void close() {
        setup.close();
    }
}

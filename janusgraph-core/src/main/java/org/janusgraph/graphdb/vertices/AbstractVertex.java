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

package org.janusgraph.graphdb.vertices;

import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.InvalidElementException;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.graphdb.internal.AbstractElement;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;
import org.janusgraph.graphdb.util.ElementHelper;
import org.janusgraph.kydsj.ContentStatus;
import org.janusgraph.kydsj.serialize.MediaData;
import org.janusgraph.kydsj.serialize.MediaDataRaw;
import org.janusgraph.kydsj.serialize.Note;

import java.util.Iterator;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex {

    private final StandardJanusGraphTx tx;
    private boolean partition;


    protected AbstractVertex(StandardJanusGraphTx tx, String id) {
        super(id);
        assert tx != null;
        this.tx = tx;
    }

    @Override
    public final InternalVertex it() {
        if (tx.isOpen())
            return this;

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(longId());
        if (next == null) throw InvalidElementException.removedException(this);
        else return next;
    }

    @Override
    public final StandardJanusGraphTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    public final boolean isTxOpen() {
        return tx.isOpen();
    }

    @Override
    public String getCompareId() {
        if(this.isPartition()) {
            if (tx.isPartitionedVertex(this)) return tx.getIdInspector().getCanonicalVertexId(longId());
            else return longId();
        }else{
            return longId();
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }

    protected final void verifyAccess() {
        if (isRemoved()) {
            throw InvalidElementException.removedException(this);
        }
    }

	/* ---------------------------------------------------------------
     * Changing Edges
	 * ---------------------------------------------------------------
	 */

    @Override
    public synchronized void remove() {
        verifyAccess();
//        if (isRemoved()) return; //Remove() is idempotent
        Iterator<JanusGraphRelation> iterator = it().query().noPartitionRestriction().relations().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        //Remove all system types on the vertex
        for (JanusGraphRelation r : it().query().noPartitionRestriction().system().relations()) {
            r.remove();
        }

        //删除附件
        Iterator<MediaData> attachments = this.attachments();
        while (attachments.hasNext()){
            MediaData mediaData=attachments.next();
            mediaData.remove();
        }

        //删除注释
        Iterator<Note> notes = this.notes();
        while (notes.hasNext()){
            Note note=notes.next();
            note.remove();
        }
    }

	/* ---------------------------------------------------------------
	 * JanusGraphRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */

    @Override
    public String label() {
        return vertexLabel().name();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(),null);
    }

    @Override
    public VertexLabel vertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        verifyAccess();
        return tx().query(this);
    }

    /**
     * 给对象添加注释信息
     * @param note
     */
    public void note(Note note){
        tx().addNote(it(),note);
    }

    /**
     * 给对象添加附件信息
     * @param mediaData
     */
    public void attachment(MediaData mediaData){
        tx().addAttachment(it(),mediaData);
    }

    @Override
    public byte[] getLargeCellContent(String fileName){
        return tx().getLargeCellContent(fileName);
    }

    @Override
    public ContentStatus getContentStatus(String fileName){
        return tx().getContentStatus(fileName);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        return (O)property(key.name()).orElse(null);
    }

	/* ---------------------------------------------------------------
	 * Convenience Methods for JanusGraphElement Creation
	 * ---------------------------------------------------------------
	 */

    public<V> JanusGraphVertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return property(null, key, value, keyValues);
    }

    @Override
    public <V> JanusGraphVertexProperty<V> property(@Nullable final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if(key.equals(BaseKey.VertexAttachment.name())) {
            JanusGraphVertexProperty p = tx().addAttachment(cardinality, it(), BaseKey.VertexAttachment, value);
            return p;
        }else if(key.equals(BaseKey.VertexNote.name())) {
            JanusGraphVertexProperty p = tx().addNote(cardinality, it(), BaseKey.VertexNote, value);
            return p;
        }else{
            PropertyKey propertyKey = tx().getOrCreatePropertyKey(key, value, cardinality);
            if (propertyKey == null) {
                return JanusGraphVertexProperty.empty();
            }
            VertexProperty.Cardinality vCardinality = cardinality == null ? propertyKey.cardinality().convert() : cardinality;
            if (value == null) {
                if (vCardinality.equals(VertexProperty.Cardinality.single)) {
                    // putting null value with SINGLE cardinality is equivalent to removing existing value
                    properties(key).forEachRemaining(Property::remove);
                } else {
                    // simply ignore this mutation
                    assert vCardinality.equals(VertexProperty.Cardinality.list) || vCardinality.equals(VertexProperty.Cardinality.set);
                }
                return JanusGraphVertexProperty.empty();
            }
            JanusGraphVertexProperty<V> p = tx().addProperty(vCardinality, it(), propertyKey, value);
            ElementHelper.attachProperties(p,keyValues);
            return p;
        }
    }

    public <V> JanusGraphVertexProperty<V> trsProperty(final String key, final V value, final Object... keyValues) {
        if(key.equals(BaseKey.VertexAttachment.name())) {
            JanusGraphVertexProperty p = tx().addAttachment(it(), BaseKey.VertexAttachment, value);
            return p;
        }else if(key.equals(BaseKey.VertexNote.name())) {
            JanusGraphVertexProperty p = tx().addNote(it(), BaseKey.VertexNote, value);
            return p;
        }else{
            PropertyKey propertyKey = tx().getPropertyKey(key);
            if (propertyKey == null) {
                return JanusGraphVertexProperty.empty();
            }
            VertexProperty.Cardinality vCardinality = propertyKey.cardinality().convert();
            if (value == null) {
                if (vCardinality.equals(VertexProperty.Cardinality.single)) {
                    // putting null value with SINGLE cardinality is equivalent to removing existing value
                    properties(key).forEachRemaining(Property::remove);
                } else {
                    // simply ignore this mutation
                    assert vCardinality.equals(VertexProperty.Cardinality.list) || vCardinality.equals(VertexProperty.Cardinality.set);
                }
                return JanusGraphVertexProperty.empty();
            }else {
                JanusGraphVertexProperty<V> p = tx().addTrsProperty(vCardinality, it(), propertyKey, value, null);
                ElementHelper.attachProperties(p, keyValues);
                return p;
            }
        }
    }

    @Override
    public JanusGraphEdge addEdge(String label, Vertex vertex, Object... keyValues) {
        Preconditions.checkArgument(vertex instanceof JanusGraphVertex,"Invalid vertex provided: %s",vertex);
        final Optional<Object> idValue = org.apache.tinkerpop.gremlin.structure.util.ElementHelper.getIdValue(keyValues);
        String id = idValue.map(String.class::cast).orElse(null);
        if(StringUtils.isNotBlank(id)){
            long partition= tx().getPartitionID((InternalVertex)vertex);
            id = tx().getIdInspector().getRelationID(id, partition);
        }
        JanusGraphEdge edge = tx().addEdge(id,it(), (JanusGraphVertex) vertex, tx().getOrCreateEdgeLabel(label));
        ElementHelper.attachProperties(edge,keyValues);
        return edge;
    }

    public Iterator<Edge> edges(Direction direction, String... labels) {
        return (Iterator)query().direction(direction).labels(labels).edges().iterator();
    }

    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        return (Iterator)query().direction(Direction.OUT).keys(keys).properties().iterator();
    }

    public Iterator<Note> notes(String ... keys) {
        Iterator<Note> iterator = tx().getNotes(longId(),keys);
        return new Iterator<Note>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Note next() {
                Note next = iterator.next();
                next.setVertex(it());
                return next;
            }
        };
    }

    public Iterator<MediaData> attachments(String ... keys) {
        Iterator<MediaData> iterator = tx().getMediaDatas(longId(),keys);
        return new Iterator<MediaData>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MediaData next() {
                MediaData next = iterator.next();
                next.setVertex(it());
                return next;
            }
        };
    }
    public Iterator<MediaDataRaw> attachmentRaws(String ... keys) {
        Iterator<MediaDataRaw> iterator = tx().getMediaDataRaws(longId(),keys);
        return new Iterator<MediaDataRaw>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public MediaDataRaw next() {
                MediaDataRaw next = iterator.next();
                next.setVertex(it());
                return next;
            }
        };
    }

    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return (Iterator)query().direction(direction).labels(edgeLabels).vertices().iterator();

    }

    public boolean isPartition() {
        return partition;
    }

    public void setPartition(boolean partition) {
        this.partition = partition;
    }
}

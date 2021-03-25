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

package org.janusgraph.graphdb.database;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectObjectIdentityHashMap;
import com.carrotsearch.hppc.ObjectSet;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.*;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.util.MD5Util;
import org.janusgraph.util.datastructures.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.janusgraph.graphdb.database.idhandling.IDHandler.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class EdgeSerializer implements RelationReader {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(EdgeSerializer.class);


    private static final int DEFAULT_COLUMN_CAPACITY = 60;
    private static final int DEFAULT_CAPACITY = 128;

    private final Serializer serializer;

    public EdgeSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public RelationCache readRelation(Entry data, boolean parseHeaderOnly, TypeInspector tx) {
        RelationCache map = data.getCache();
        if (map == null || !(parseHeaderOnly || map.hasProperties())) {
            map = parseRelation(data, parseHeaderOnly, tx);
            data.setCache(map);
        }
        return map;
    }

    public Direction parseDirection(Entry data) {
        RelationCache map = data.getCache();
        if (map != null) return map.direction;
        return IDHandler.readRelationType(data.asReadBuffer()).dirID.getDirection();
    }

    @Override
    public RelationCache parseRelation(Entry data, boolean excludeProperties, TypeInspector tx) {
        ReadBuffer in = data.asReadBuffer();

        RelationTypeParse typeAndDir = IDHandler.readRelationType(in);

        String typeId = typeAndDir.typeId;
        Direction dir = typeAndDir.dirID.getDirection();

        RelationType relationType = tx.getExistingRelationType(typeId);
        InternalRelationType def = (InternalRelationType) relationType;
        Multiplicity multiplicity = def.multiplicity();
        String[] keySignature = def.getSortKey();

        String relationId;
        Object other;
        int startKeyPos = in.getPosition();
        int endKeyPos = 0;
        if (relationType.isEdgeLabel()) {
            String otherVertexId;
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    //otherVertexId = VariableLong.readPositive(in);
                    otherVertexId= serializer.readObjectNotNull(in,String.class);
                    relationId = serializer.readObjectNotNull(in,String.class);
                } else {
                    //in.movePositionTo(data.getValuePosition());
                    //otherVertexId = VariableLong.readPositiveBackward(in);
                    otherVertexId=serializer.readObjectNotNull(in,String.class);
                    in.movePositionTo(data.getValuePosition());
                    relationId = serializer.readObjectNotNull(in,String.class);
                }
                //relationId = VariableLong.readPositive(in);
            } else {
                //in.movePositionTo(data.getValuePosition());
                //relationId = VariableLong.readPositiveBackward(in);
                //otherVertexId = VariableLong.readPositiveBackward(in);
                otherVertexId = serializer.readObjectNotNull(in,String.class);
                relationId=serializer.readObjectNotNull(in,String.class);
                endKeyPos = in.getPosition();
                in.movePositionTo(data.getValuePosition());

                //VariableLong.writePositiveBackward(out, otherVertexId);
                //VariableLong.writePositiveBackward(out, relationId);
            }
            other = otherVertexId;
        } else {
            assert relationType.isPropertyKey();
            PropertyKey key = (PropertyKey) relationType;

            if (multiplicity.isConstrained()) {
                other = readPropertyValue(in,key);
               // relationId = VariableLong.readPositive(in);
                relationId = serializer.readObjectNotNull(in,String.class);
            } else {
                //in.movePositionTo(data.getValuePosition());
                //relationId = VariableLong.readPositiveBackward(in);
                endKeyPos = in.getPosition();
                relationId = serializer.readObjectNotNull(in,String.class);
                in.movePositionTo(data.getValuePosition());
                other = readPropertyValue(in,key);
            }
            Preconditions.checkNotNull(other,
                "Encountered error in deserializer [null value returned]. Check serializer compatibility.");
        }

        if (!excludeProperties) {

            ObjectObjectIdentityHashMap<String,Object> properties = new ObjectObjectIdentityHashMap<>(4);

            if (!multiplicity.isConstrained() && keySignature.length > 0) {
                int currentPos = in.getPosition();
                //Read sort key which only exists if type is not unique in this direction
                assert endKeyPos > startKeyPos;
                int keyLength = endKeyPos - startKeyPos; //after reading the ids, we are on the last byte of the key
                in.movePositionTo(startKeyPos);
                ReadBuffer inKey = in;
                if (def.getSortOrder() == Order.DESC) inKey = in.subrange(keyLength, true);
                readInlineTypes(keySignature, properties, inKey, tx, InlineType.KEY);
                in.movePositionTo(currentPos);
            }

            //read value signature
            readInlineTypes(def.getSignature(), properties, in, tx, InlineType.SIGNATURE);

            //Third: read rest
            while (in.hasRemaining()) {
                PropertyKey type = tx.getExistingPropertyKey(IDHandler.readInlineRelationType(in));
                Object propertyValue = readInline(in, type, InlineType.NORMAL);
                assert propertyValue != null;
                properties.put(type.longId(), propertyValue);
            }

            if (data.hasMetaData()) {
                for (Map.Entry<EntryMetaData,Object> metas : data.getMetaData().entrySet()) {
                    ImplicitKey key = ImplicitKey.MetaData2ImplicitKey.get(metas.getKey());
                    if (key != null) {
                        assert metas.getValue() != null;
                        properties.put(key.longId(),metas.getValue());
                    }
                }
            }

            return new RelationCache(dir, typeId, relationId, other, properties);
        } else {
            return new RelationCache(dir, typeId, relationId, other);
        }
    }

    private void readInlineTypes(String[] keyIds, ObjectObjectIdentityHashMap<String,Object> properties, ReadBuffer in, TypeInspector tx,
                                 InlineType inlineType) {
        for (String keyId : keyIds) {
            PropertyKey keyType = tx.getExistingPropertyKey(keyId);
            Object value = readInline(in, keyType, inlineType);
            if (value != null) properties.put(keyId, value);
        }
    }

    private Object readInline(ReadBuffer read, PropertyKey key, InlineType inlineType) {
        return readPropertyValue(read, key, inlineType);
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key) {
        return readPropertyValue(read,key,InlineType.NORMAL);
    }

    private Object readPropertyValue(ReadBuffer read, PropertyKey key, InlineType inlineType) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            return serializer.readClassAndObject(read);
        } else {
            if (inlineType.writeByteOrdered())
                return serializer.readObjectByteOrder(read, key.dataType());
            else
                return serializer.readObject(read, key.dataType());
        }
    }

    private static DirectionID getDirID(Direction dir, RelationCategory rt) {
        switch (rt) {
            case PROPERTY:
                assert dir == Direction.OUT;
                return DirectionID.PROPERTY_DIR;

            case EDGE:
                switch (dir) {
                    case OUT:
                        return DirectionID.EDGE_OUT_DIR;

                    case IN:
                        return DirectionID.EDGE_IN_DIR;

                    default:
                        throw new IllegalArgumentException("Invalid direction: " + dir);
                }

            default:
                throw new IllegalArgumentException("Invalid relation type: " + rt);
        }
    }

    public Entry writeRelation(InternalRelation relation, int position, TypeInspector tx) {
        return writeRelation(relation, (InternalRelationType) relation.getType(), position, tx);
    }

    public StaticArrayEntry writeRelation(InternalRelation relation, InternalRelationType type, int position,
                                          TypeInspector tx) {
        assert type==relation.getType() || (type.getBaseType() != null
                && type.getBaseType().equals(relation.getType()));
        Direction dir = EdgeDirection.fromPosition(position);
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));
        String typeId = type.longId();
        DirectionID dirID = getDirID(dir, relation.isProperty() ? RelationCategory.PROPERTY : RelationCategory.EDGE);

        DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
        int valuePosition;
        IDHandler.writeRelationType(out, typeId, dirID, type.isInvisibleType());
        Multiplicity multiplicity = type.multiplicity();

        String[] sortKey = type.getSortKey();
        assert !multiplicity.isConstrained() || sortKey.length==0: type.name();
        int keyStartPos = out.getPosition();
        if (!multiplicity.isConstrained()) {
            writeInlineTypes(sortKey, relation, out, tx, InlineType.KEY);
        }
        int keyEndPos = out.getPosition();

        String relationId = relation.longId();

        //How multiplicity is handled for edges and properties is slightly different
        if (relation.isEdge()) {
            String otherVertexId = relation.getVertex((position + 1) % 2).longId();
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    valuePosition = out.getPosition();
                    //VariableLong.writePositive(out, otherVertexId);
                    out.writeObjectNotNull(otherVertexId);
                    out.writeObjectNotNull(relationId);

                } else {
                    //VariableLong.writePositiveBackward(out, otherVertexId);
                    out.writeObjectNotNull(otherVertexId);
                    valuePosition = out.getPosition();
                    out.writeObjectNotNull(relationId);
                }
                //VariableLong.writePositive(out, relationId);
            } else {
                //VariableLong.writePositiveBackward(out, otherVertexId);
                //VariableLong.writePositiveBackward(out, relationId);
                out.writeObjectNotNull(otherVertexId);
                out.writeObjectNotNull(relationId);
                valuePosition = out.getPosition();
            }
        } else {
            assert relation.isProperty();
            Preconditions.checkArgument(relation.isProperty());
            Object value = ((JanusGraphVertexProperty) relation).value();
            Preconditions.checkNotNull(value);
            PropertyKey key = (PropertyKey) type;
            assert key.dataType().isInstance(value);

            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) { //Cardinality=SINGLE
                    valuePosition = out.getPosition();
                    writePropertyValue(out,key,value);
                } else { //Cardinality=SET
                    writePropertyValue(out,key,value);
                    valuePosition = out.getPosition();
                }
                //VariableLong.writePositive(out, relationId);
                out.writeObjectNotNull(relationId);
            } else {
                assert multiplicity.getCardinality()== Cardinality.LIST;
                //VariableLong.writePositiveBackward(out, relationId);
                out.writeObjectNotNull(relationId);
                valuePosition = out.getPosition();
                writePropertyValue(out,key,value);
            }
        }

        //Write signature
        String[] signature = type.getSignature();
        writeInlineTypes(signature, relation, out, tx, InlineType.SIGNATURE);

        //Write remaining properties
        ObjectSet<String> writtenTypes = new ObjectHashSet<>(sortKey.length + signature.length);
        if (sortKey.length > 0 || signature.length > 0) {
            for (String id : sortKey) writtenTypes.add(id);
            for (String id : signature) writtenTypes.add(id);
        }
        ObjectArrayList<String> remainingTypes = new ObjectArrayList(8);
        for (PropertyKey t : relation.getPropertyKeysDirect()) {
            if (!(t instanceof ImplicitKey) && !writtenTypes.contains(t.longId())) {
                remainingTypes.add(t.longId());
            }
        }
        //Sort types before writing to ensure that value is always written the same way
        String[] remaining = remainingTypes.toArray(String.class);
        Arrays.sort(remaining);
        for (String tid : remaining) {
            PropertyKey t = tx.getExistingPropertyKey(tid);
            writeInline(out, t, relation.getValueDirect(t), InlineType.NORMAL);
        }
        assert valuePosition>0;

        return new StaticArrayEntry(type.getSortOrder() == Order.DESC ?
                                    out.getStaticBufferFlipBytes(keyStartPos, keyEndPos) :
                                    out.getStaticBuffer(), valuePosition);
    }


    public List<StaticArrayEntry> writeMulitPropertyProperties(StandardVertexProperty standardVertexProperty,
                                                          InternalRelationType type,
                                                          StandardJanusGraphTx tx) {
        Iterator<? extends Property<Object>> properties = standardVertexProperty.properties();
        List<StaticArrayEntry> entries=new ArrayList<>();
        String typeId = type.longId();
        String relationId = standardVertexProperty.longId();
        String propertyValueMD5 = MD5Util.getMD5(standardVertexProperty.value());
        while (properties.hasNext()){
            Property<Object> property=properties.next();
            String key = property.key();
            PropertyKey propertyPropertyKey = tx.getPropertyKey(key);
            if(propertyPropertyKey.isPropertyKey()){
                Object valueDirect = property.value();
                DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
                out.writeObjectNotNull(typeId);
                out.writeObjectNotNull(propertyValueMD5);
                out.writeObjectNotNull(propertyPropertyKey.longId());
                writePropertyValue(out, propertyPropertyKey, valueDirect);
                final int valuePosition = out.getPosition();
                out.writeObjectNotNull(relationId);
                StaticArrayEntry propertyPropertyEntry = new StaticArrayEntry(out.getStaticBuffer(), valuePosition);
                entries.add(propertyPropertyEntry);
            }
        }
        return entries;
    }
    /**
     *
     * @param standardVertexProperty 属性
     * @param type 属性类型
     * @param tx
     * @return
     */
    public List<StaticArrayEntry> writePropertyProperties(StandardVertexProperty standardVertexProperty,
                                                    InternalRelationType type,
                                                    StandardJanusGraphTx tx) {
        assert type==standardVertexProperty.getType() || (type.getBaseType() != null
            && type.getBaseType().equals(standardVertexProperty.getType()));
        List<StaticArrayEntry> entries=new ArrayList<>();
        if (standardVertexProperty.isProperty()) {
            String typeId = type.longId();
            //Multiplicity multiplicity = type.multiplicity();
            String relationId = standardVertexProperty.longId();
            String propertyValueMD5 = MD5Util.getMD5(standardVertexProperty.value());
            Iterable<PropertyKey> propertyKeysDirect = standardVertexProperty.getPropertyKeysDirect();
            for (PropertyKey propertyPropertyKey : propertyKeysDirect) {
                if (propertyPropertyKey.cardinality() == Cardinality.SET) {
                    Object valueDirect = standardVertexProperty.getValueDirect(propertyPropertyKey);
                    DataOutput out = serializer.getDataOutput(DEFAULT_CAPACITY);
                    out.writeObjectNotNull(typeId);
                    out.writeObjectNotNull(propertyValueMD5);
                    out.writeObjectNotNull(propertyPropertyKey.longId());
                    writePropertyValue(out, propertyPropertyKey, valueDirect);
                    final int valuePosition = out.getPosition();
                    out.writeObjectNotNull(relationId);
                    StaticArrayEntry propertyPropertyEntry = new StaticArrayEntry(out.getStaticBuffer(), valuePosition);
                    entries.add(propertyPropertyEntry);
                }
            }
        }
        return entries;
    }

    private enum InlineType {

        KEY, SIGNATURE, NORMAL;

        public boolean writeInlineKey() {
            return this==NORMAL;
        }

        public boolean writeByteOrdered() {
            return this==KEY;
        }

    }

    private void writeInlineTypes(String[] keyIds, InternalRelation relation, DataOutput out, TypeInspector tx,
                                  InlineType inlineType) {
        for (String keyId : keyIds) {
            PropertyKey t = tx.getExistingPropertyKey(keyId);
            writeInline(out, t, relation.getValueDirect(t), inlineType);
        }
    }

    private void writeInline(DataOutput out, PropertyKey inlineKey, Object value, InlineType inlineType) {
        assert inlineType.writeInlineKey() || !InternalAttributeUtil.hasGenericDataType(inlineKey);

        if (inlineType.writeInlineKey()) {
            IDHandler.writeInlineRelationType(out, inlineKey.longId());
        }

        writePropertyValue(out,inlineKey,value, inlineType);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value) {
        writePropertyValue(out,key,value,InlineType.NORMAL);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value, InlineType inlineType) {
        if (InternalAttributeUtil.hasGenericDataType(key)) {
            assert !inlineType.writeByteOrdered();
            out.writeClassAndObject(value);
        } else {
            assert value==null || value.getClass().equals(key.dataType());
            if (inlineType.writeByteOrdered()) out.writeObjectByteOrder(value, key.dataType());
            else out.writeObject(value, key.dataType());
        }
    }

    public SliceQuery getQuery(RelationCategory resultType, boolean querySystemTypes) {
        Preconditions.checkNotNull(resultType);
        StaticBuffer[] bound = getBounds(resultType, querySystemTypes);
        return new SliceQuery(bound[0], bound[1]);
    }

    public SliceQuery getQuery(InternalRelationType type, Direction dir, TypedInterval[] sortKey) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dir);
        Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));


        StaticBuffer sliceStart = null, sliceEnd = null;
        RelationCategory rt = type.isPropertyKey() ? RelationCategory.PROPERTY : RelationCategory.EDGE;
        if (dir == Direction.BOTH) {
            assert type.isEdgeLabel();
            sliceStart = IDHandler.getRelationType(type.longId(), getDirID(Direction.OUT, rt), type.isInvisibleType());
            sliceEnd = IDHandler.getRelationType(type.longId(), getDirID(Direction.IN, rt), type.isInvisibleType());
            assert sliceStart.compareTo(sliceEnd)<0;
            sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
        } else {
            DirectionID dirID = getDirID(dir, rt);

            DataOutput colStart = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            DataOutput colEnd = serializer.getDataOutput(DEFAULT_COLUMN_CAPACITY);
            IDHandler.writeRelationType(colStart, type.longId(), dirID, type.isInvisibleType());
            IDHandler.writeRelationType(colEnd, type.longId(), dirID, type.isInvisibleType());

            String[] sortKeyIDs = type.getSortKey();
            Preconditions.checkArgument(sortKey.length >= sortKeyIDs.length);
            assert colStart.getPosition() == colEnd.getPosition();
            int keyStartPos = colStart.getPosition();
            int keyEndPos = -1;
            for (int i = 0; i < sortKey.length && sortKey[i] != null; i++) {
                PropertyKey propertyKey = sortKey[i].key;
                Interval interval = sortKey[i].interval;

                if (i>=sortKeyIDs.length) {
                    assert !type.multiplicity().isUnique(dir);
                    assert propertyKey==ImplicitKey.JANUSGRAPHID || propertyKey==ImplicitKey.ADJACENT_ID;
                    assert propertyKey!=ImplicitKey.ADJACENT_ID || (i==sortKeyIDs.length);
                    assert propertyKey!=ImplicitKey.JANUSGRAPHID || (!type.multiplicity().isConstrained() &&
                                                  (i==sortKeyIDs.length && propertyKey.isPropertyKey()
                                                      || i==sortKeyIDs.length+1 && propertyKey.isEdgeLabel() ));
                    assert colStart.getPosition()==colEnd.getPosition();
                    assert interval==null || interval.isPoints();
                    keyEndPos = colStart.getPosition();

                } else {
                    assert !type.multiplicity().isConstrained();
                    assert propertyKey.longId().equals(sortKeyIDs[i]);
                }

                if (interval == null || interval.isEmpty()) {
                    break;
                }
                if (interval.isPoints()) {
                    if (propertyKey==ImplicitKey.JANUSGRAPHID || propertyKey==ImplicitKey.ADJACENT_ID) {
                        assert !type.multiplicity().isUnique(dir);
                        //VariableLong.writePositiveBackward(colStart, (Long)interval.getStart());
                       // VariableLong.writePositiveBackward(colEnd, (Long)interval.getEnd());
                        colStart.writeObjectNotNull(interval.getStart());
                        colEnd.writeObjectNotNull(interval.getEnd());
                    } else {
                        writeInline(colStart, propertyKey, interval.getStart(), InlineType.KEY);
                        writeInline(colEnd, propertyKey, interval.getEnd(), InlineType.KEY);
                    }
                } else {
                    if (interval.getStart() != null)
                        writeInline(colStart, propertyKey, interval.getStart(), InlineType.KEY);
                    if (interval.getEnd() != null)
                        writeInline(colEnd, propertyKey, interval.getEnd(), InlineType.KEY);

                    switch (type.getSortOrder()) {
                        case ASC:
                            sliceStart = colStart.getStaticBuffer();
                            sliceEnd = colEnd.getStaticBuffer();
                            if (!interval.startInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            if (interval.endInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            break;

                        case DESC:
                            sliceEnd = colStart.getStaticBufferFlipBytes(keyStartPos,colStart.getPosition());
                            sliceStart = colEnd.getStaticBufferFlipBytes(keyStartPos,colEnd.getPosition());
                            if (interval.startInclusive()) sliceEnd = BufferUtil.nextBiggerBuffer(sliceEnd);
                            if (!interval.endInclusive()) sliceStart = BufferUtil.nextBiggerBuffer(sliceStart);
                            break;

                        default:
                            throw new AssertionError(type.getSortOrder().toString());
                    }

                    assert sliceStart.compareTo(sliceEnd)<=0;
                    break;
                }
            }
            if (sliceStart==null) {
                assert sliceEnd==null && colStart.getPosition()==colEnd.getPosition();
                if (keyEndPos<0) keyEndPos=colStart.getPosition();
                switch (type.getSortOrder()) {
                    case ASC:
                        sliceStart = colStart.getStaticBuffer();
                        break;

                    case DESC:
                        sliceStart = colStart.getStaticBufferFlipBytes(keyStartPos,keyEndPos);
                        break;

                    default:
                        throw new AssertionError(type.getSortOrder().toString());
                }
                sliceEnd = BufferUtil.nextBiggerBuffer(sliceStart);
            }
        }
        return new SliceQuery(sliceStart, sliceEnd);
    }

    public static class TypedInterval {
        public final PropertyKey key;
        public final Interval interval;


        public TypedInterval(PropertyKey key, Interval interval) {
            this.key = key;
            this.interval = interval;
        }
    }

}

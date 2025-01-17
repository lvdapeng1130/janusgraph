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

package org.janusgraph.graphdb.relations;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.Iterables;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PropertyPropertyInfo;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertexProperty extends AbstractVertexProperty {

    public CacheVertexProperty(String id, PropertyKey key, InternalVertex start, Object value, Entry data) {
        super(id, key, start.it(), value);
        this.data = data;
    }

    //############## Similar code as CacheEdge but be careful when copying #############################

    private final Entry data;

    //属性的多值属性比如dsr
    private List<PropertyPropertyInfo> multiPropertyProperties;

    public List<PropertyPropertyInfo> getMultiPropertyProperties() {
        return multiPropertyProperties;
    }

    public void setMultiPropertyProperties(List<PropertyPropertyInfo> multiPropertyProperties) {
        this.multiPropertyProperties = multiPropertyProperties;
    }

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final String id = longId();
            it = Iterables.getOnlyElement(startVertex.getAddedRelations(
                internalRelation -> (internalRelation instanceof StandardVertexProperty) && ((StandardVertexProperty) internalRelation).getPreviousID() == id), null);
        }

        return (it != null) ? it : super.it();
    }

    private void copyProperties(InternalRelation to) {
        for (ObjectObjectCursor<String,Object> entry : getPropertyMap()) {
            PropertyKey type = tx().getExistingPropertyKey(entry.key);
            if (!(type instanceof ImplicitKey))
                to.setPropertyDirect(type, entry.value);
        }
    }

    private synchronized InternalRelation update() {
        StandardVertexProperty copy = new StandardVertexProperty(longId(), propertyKey(), getVertex(0), value(), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        String id = type.getConsistencyModifier() != ConsistencyModifier.FORK ? longId() : null;
        StandardVertexProperty u = (StandardVertexProperty) tx().addProperty(getVertex(0), propertyKey(), value(), id);
        u.setPreviousID(longId());
        copyProperties(u);
        return u;
    }

    private synchronized InternalRelation updateExpend() {
        StandardVertexProperty copy = new StandardVertexProperty(longId(), propertyKey(), getVertex(0), value(), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        String id = type.getConsistencyModifier() != ConsistencyModifier.FORK ? longId() : null;
        StandardVertexProperty u = (StandardVertexProperty) tx().addProperty(getVertex(0), propertyKey(), value(), id);
        u.setPreviousID(longId());
        copyPropertiesExpend(u);
        return u;
    }

    private void copyPropertiesExpend(StandardVertexProperty to) {
        boolean existMuiltValue=false;
        for (ObjectObjectCursor<String,Object> entry : getPropertyMap()) {
            PropertyKey type = tx().getExistingPropertyKey(entry.key);
            if (!(type instanceof ImplicitKey)) {
                to.setPropertyDirect(type, entry.value);
                if (type.cardinality() == org.janusgraph.core.Cardinality.SET) {
                    existMuiltValue=true;
                }
            }
        }
        if (existMuiltValue) {
            InternalVertex vertex = this.getVertex(0);
            if(vertex instanceof CacheVertex){
                CacheVertex cacheVertex=(CacheVertex) vertex;
                Collection<SimpleJanusGraphProperty> propertyProperties = cacheVertex.findPropertyProperties(this);
                if(propertyProperties!=null){
                    for (SimpleJanusGraphProperty pp : propertyProperties) {
                        if(pp.getLifeCycle()!=ElementLifeCycle.Removed) {
                            to.setMultiPropertyDirect(pp.propertyKey(), pp.value());
                        }
                    }
                }
            }else {
                Iterable<SimpleJanusGraphProperty> propertyProperties = tx().getPropertyProperties(this, this.value(), null);
                for (SimpleJanusGraphProperty pp : propertyProperties) {
                    if(pp.getLifeCycle()!=ElementLifeCycle.Removed) {
                        to.setMultiPropertyDirect(pp.propertyKey(), pp.value());
                    }
                }
            }
        }
    }


    private RelationCache getPropertyMap() {
        RelationCache map = data.getCache();
        if (map == null || !map.hasProperties()) {
            map = RelationConstructor.readRelationCache(data, tx());
        }
        return map;
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return getPropertyMap().get(key.longId());
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        RelationCache map = getPropertyMap();
        List<PropertyKey> types = new ArrayList<>(map.numProperties());

        for (ObjectObjectCursor<String,Object> entry : map) {
            types.add(tx().getExistingPropertyKey(entry.key));
        }
        return types;
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        update().setPropertyDirect(key, value);
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        return update().removePropertyDirect(key);
    }

    @Override
    public <O> O removePropertyDsr(PropertyKey key, Object value) {
        return updateExpend().removePropertyDsr(key,value);
    }

    @Override
    public byte getLifeCycle() {
        if ((getVertex(0).hasRemovedRelations() || getVertex(0).isRemoved()) && tx().isRemovedRelation(longId()))
            return ElementLifeCycle.Removed;
        else return ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        if (!isRemoved()) {
            tx().removeRelation(this);
        }// else throw InvalidElementException.removedException(this);
    }


}

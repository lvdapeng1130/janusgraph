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

import com.google.common.base.Preconditions;
import org.janusgraph.core.*;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractVertexProperty<V> extends AbstractTypedRelation implements JanusGraphVertexProperty<V> {

    private InternalVertex vertex;
    private final Object value;

    public AbstractVertexProperty(String id, PropertyKey type, InternalVertex vertex, Object value) {
        super(id, type);
        this.vertex = Preconditions.checkNotNull(vertex, "null vertex");
        this.value = Preconditions.checkNotNull(value, "null value for property key %s",type);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(pos==0 && vertex!=null && this.vertex.equals(vertex));
        this.vertex=vertex;
    }

    @Override
    public InternalVertex getVertex(int pos) {
        if (pos==0) return vertex;
        else throw new IllegalArgumentException("Invalid position: " + pos);
    }

    @Override
    public JanusGraphTransaction graph() {
        return vertex.graph();
    }

    @Override
    public Property<V> lastProperty(final String key){
        final Iterator<? extends Property<V>> iterator = super.properties(key);
        return iterator.hasNext() ? iterator.next() : Property.<V>empty();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        if(type.isPropertyKey()) {
            Set<Property<U>> propertySet=new HashSet<>();
            PropertyKey key=(PropertyKey)type;
            if (key.cardinality() == org.janusgraph.core.Cardinality.SET) {
                InternalVertex vertex = this.getVertex(0);
                Iterable<SimpleJanusGraphProperty> propertyProperties = tx().getPropertyProperties(this, this.value(),propertyKeys);
                for(SimpleJanusGraphProperty pp:propertyProperties){
                    propertySet.add(pp);
                }
            }
            Iterator<Property<U>> properties = super.properties(propertyKeys);
            if(properties!=null){
                while (properties.hasNext()){
                    Property<U> pp = properties.next();
                    propertySet.add(pp);
                }
            }
            return propertySet.iterator();
        }else {
            return super.properties(propertyKeys);
        }
    }

    @Override
    public final int getArity() {
        return 1;
    }

    @Override
    public final int getLen() {
        return 1;
    }

    @Override
    public JanusGraphVertex element() {
        return vertex;
    }

    @Override
    public V value() {
        return (V)value;
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean isEdge() {
        return false;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

}

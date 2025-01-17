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

import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardEdge extends AbstractEdge implements StandardRelation, ReassignableRelation {

    private static final Map<PropertyKey, Object> EMPTY_PROPERTIES = Collections.emptyMap();

    private byte lifecycle;
    private String previousID;
    private volatile Map<PropertyKey, Object> properties = EMPTY_PROPERTIES;

    public StandardEdge(String id, EdgeLabel label, InternalVertex start, InternalVertex end, byte lifecycle) {
        super(id, label, start, end);
        this.lifecycle = lifecycle;
    }

    @Override
    public String getPreviousID() {
        return previousID;
    }

    @Override
    public void setPreviousID(String previousID) {
        Preconditions.checkArgument(StringUtils.isNotBlank(previousID));
        this.previousID = previousID;
    }

    @Override
    public <O> O getValueDirect(PropertyKey type) {
        return (O) properties.get(type);
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        Preconditions.checkArgument(!(key instanceof ImplicitKey), "Cannot use implicit type [%s] when setting property", key.name());
        if (properties == EMPTY_PROPERTIES) {
            if (tx().getConfiguration().isSingleThreaded()) {
                properties = new HashMap<>(5);
            } else {
                synchronized (this) {
                    if (properties == EMPTY_PROPERTIES) {
                        properties = Collections.synchronizedMap(new HashMap<>(5));
                    }
                }
            }
        }
        tx().checkPropertyConstraintForEdgeOrCreatePropertyConstraint(this, key);
        properties.put(key, value);
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        return new ArrayList<>(properties.keySet());
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        if (!properties.isEmpty())
            return (O) properties.remove(key);
        else return null;
    }

    @Override
    public <O> O removePropertyDsr(PropertyKey key, Object value) {
       return null;
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public synchronized void remove() {
        if (!ElementLifeCycle.isRemoved(lifecycle)) {
            tx().removeRelation(this);
            lifecycle = ElementLifeCycle.update(lifecycle, ElementLifeCycle.Event.REMOVED);
        } //else throw InvalidElementException.removedException(this);
    }

}

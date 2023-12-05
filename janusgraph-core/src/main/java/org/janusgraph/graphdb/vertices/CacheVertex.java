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

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PropertyPropertyInfo;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.relations.AbstractVertexProperty;
import org.janusgraph.graphdb.relations.SimpleJanusGraphProperty;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.util.MD5Util;
import org.janusgraph.util.datastructures.Retriever;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheVertex extends StandardVertex {
    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    // We use a normal map with synchronization since the likelihood of contention
    // is super low in a single transaction
    protected final Map<SliceQuery, EntryList> queryCache;
    protected Multimap<String,SimpleJanusGraphProperty> propertiyOfProperties;

    public CacheVertex(StandardJanusGraphTx tx, String id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<>(4);
    }

    public void refresh() {
        synchronized (queryCache) {
            queryCache.clear();
        }
    }

    protected void addToQueryCache(final SliceQuery query, final EntryList entries) {
        synchronized (queryCache) {
            //TODO: become smarter about what to cache and when (e.g. memory pressure)
            queryCache.put(query, entries);
        }
    }

    protected int getQueryCacheSize() {
        synchronized (queryCache) {
            return queryCache.size();
        }
    }

    @Override
    public EntryList loadRelations(final SliceQuery query, final Retriever<SliceQuery, EntryList> lookup) {
        if (isNew())
            return EntryList.EMPTY_LIST;

        EntryList result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        if (result == null) {
            //First check for super
            Map.Entry<SliceQuery, EntryList> superset = getSuperResultSet(query);
            if (superset == null || superset.getValue() == null) {
                result = lookup.get(query);
                //判断是否是查询所有属性，如果是则加载属性的属性值
                StaticBuffer[] bound = IDHandler.getBounds(RelationCategory.PROPERTY,false);
                SliceQuery propertyQuery = new SliceQuery(bound[0], bound[1]);
                if(propertyQuery.equals(query)){
                   this.loadPropertyOfProperties();
                }else if(propertiyOfProperties==null&&propertyQuery.subsumes(query)){
                    this.loadPropertyOfProperties();
                }
            } else {
                result = query.getSubset(superset.getKey(), superset.getValue());
            }
            addToQueryCache(query, result);

        }
        return result;
    }

    @Override
    public boolean hasLoadedRelations(final SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query) != null || getSuperResultSet(query) != null;
        }
    }

    private Map.Entry<SliceQuery, EntryList> getSuperResultSet(final SliceQuery query) {

        synchronized (queryCache) {
            if (queryCache.size() > 0) {
                for (Map.Entry<SliceQuery, EntryList> entry : queryCache.entrySet()) {
                    if (entry.getKey().subsumes(query)) return entry;
                }
            }
        }
        return null;
    }

    public Collection<SimpleJanusGraphProperty> findPropertyProperties(AbstractVertexProperty property){
        return this.findPropertyProperties(property,null);
    }
    public Collection<SimpleJanusGraphProperty> findPropertyProperties(AbstractVertexProperty property,final String... propertyKeys){
        if(propertiyOfProperties!=null&&propertiyOfProperties.size()>0){
            Object value = property.value();
            String propertyValueMD5 = MD5Util.getMD8(value);
            String propertyTypeId = property.propertyKey().longId();
            Collection<SimpleJanusGraphProperty> simpleJanusGraphProperties = propertiyOfProperties.get(propertyTypeId+propertyValueMD5);
            if(simpleJanusGraphProperties!=null){
                simpleJanusGraphProperties.stream().forEach(f -> f.setRelation(property));
                if(propertyKeys!=null&&propertyKeys.length>0){
                    Set<String> propertySet= Sets.newHashSet(propertyKeys);
                    return simpleJanusGraphProperties.stream().filter(f->propertySet.contains(f.propertyKey().name())).collect(Collectors.toList());
                }else {
                    return simpleJanusGraphProperties;
                }
            }
        }
        return null;
    }

    @Override
    public synchronized void remove() {
        super.remove();
        this.loadPropertyOfProperties();
    }

    public void loadPropertyOfProperties(){
        Multimap<String,SimpleJanusGraphProperty> entries= this.tx().getPropertyProperties(this);
        this.propertiyOfProperties=entries;
    }

    public Multimap<String,SimpleJanusGraphProperty> getPropertiyOfProperties() {
        return propertiyOfProperties;
    }
}

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

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.MetaAnnotatable;
import org.janusgraph.diskstorage.MetaAnnotated;

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * An index entry is a key-value pair (or field-value pair).
 * 索引条目是键值对（或字段-值对）
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexEntry implements MetaAnnotated, MetaAnnotatable {

    public final String field;
    public final Object value;
    private Date startDate;
    private Date endDate;
    private String role;
    private Set<String> dsr;
    private double[] geo;


    public IndexEntry(final String field, final Object value) {
        this(field, value, null);
    }

    public IndexEntry(final String field, final Object value, Map<EntryMetaData, Object> metadata) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(StringUtils.isNotBlank(field));

        this.field = field;
        this.value = value;

        if (metadata == null || metadata == EntryMetaData.EMPTY_METADATA)
            return;

        for (Map.Entry<EntryMetaData, Object> e : metadata.entrySet())
            setMetaData(e.getKey(), e.getValue());
    }

    //########## META DATA ############
    //copied from StaticArrayEntry

    private Map<EntryMetaData,Object> metadata = EntryMetaData.EMPTY_METADATA;

    @Override
    public synchronized Object setMetaData(EntryMetaData key, Object value) {
        if (metadata == EntryMetaData.EMPTY_METADATA)
            metadata = new EntryMetaData.Map();

        return metadata.put(key,value);
    }

    @Override
    public boolean hasMetaData() {
        return !metadata.isEmpty();
    }

    @Override
    public Map<EntryMetaData,Object> getMetaData() {
        return metadata;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Set<String> getDsr() {
        return dsr;
    }

    public void setDsr(Set<String> dsr) {
        this.dsr = dsr;
    }

    public double[] getGeo() {
        return geo;
    }

    public void setGeo(double[] geo) {
        this.geo = geo;
    }
}

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

package org.janusgraph.graphdb.types;

import org.janusgraph.core.PropertyKey;

import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MixedIndexType extends IndexType {

    ParameterIndexField[] getFieldKeys();

    ParameterIndexField getField(PropertyKey key);

    String getStoreName();

    /**
     * 设置es索引别名
     * @param aliases
     */
    void setAliases(Set<String> aliases);

    /**
     * 获取es索引别名
     * @return
     */
    Set<String> getAliases();

    /**
     * 设置创建elasticsearch索引时指定setting参数
     * @param settings
     */
    void setSettings(Map<String,Object> settings);

    /**
     * 获取创建elasticsearch索引时指定setting参数
     */
    Map<String,Object> getSettings();

}

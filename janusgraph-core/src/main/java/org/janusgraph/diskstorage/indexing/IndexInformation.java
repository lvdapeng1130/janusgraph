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

import org.janusgraph.graphdb.query.JanusGraphPredicate;

/**
 * An IndexInformation gives basic information on what a particular {@link IndexProvider} supports.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given predicate against a key with the given information
     * 索引是否支持对具有给定信息的键的给定谓词执行查询
     * @param information
     * @param janusgraphPredicate
     * @return
     */
    boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate);

    /**
     * Whether the index supports indexing a key with the given information
     * 索引是否支持使用给定信息对键进行索引
     * @param information
     * @return
     */
    boolean supports(KeyInformation information);


    /**
     * Adjusts the name of the key so that it is a valid field name that can be used in the index.
     * JanusGraph stores this information and will use the returned name in all interactions with the index.
     * <p>
     * Note, that mapped field names (either configured on a per key basis or through a global configuration)
     * are not adjusted and handed to the index verbatim.
     *调整键的名称，以便它是可以在索引中使用的有效字段名称。
     * JanusGraph存储此信息，并将在与索引的所有交互中使用返回的名称。
     * 请注意，映射字段名称（基于每个键或通过全局配置进行配置）不会调整并逐字传递给索引。
     * @param key
     * @param information
     * @return
     */
    String mapKey2Field(String key, KeyInformation information);

    /**
     * The features of this index
     * @return
     */
    IndexFeatures getFeatures();

}

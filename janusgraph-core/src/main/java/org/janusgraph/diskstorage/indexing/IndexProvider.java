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

import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * External index for querying.
 * An index can contain an arbitrary number of index stores which are updated and queried separately.
 * 用于查询的外部索引。 索引可以包含任意数量的索引存储，这些存储分别进行更新和查询。
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IndexProvider extends IndexInformation {
    /*
     * An obscure unicode character (•) provided as a convenience for implementations of {@link #mapKey2Field}, for
     * instance to replace spaces in property keys. See #777.
     */
    char REPLACEMENT_CHAR = '\u2022';

    static void checkKeyValidity(String key) {
        Preconditions.checkArgument(!StringUtils.containsAny(key, new char[]{ IndexProvider.REPLACEMENT_CHAR }),
            "Invalid key name containing reserved character %c provided: %s", IndexProvider.REPLACEMENT_CHAR, key);
    }

    /**
     * This method registers a new key for the specified index store with the given data type. This allows the IndexProvider
     * to prepare the index if necessary.
     *
     * It is expected that this method is first called with each new key to inform the index of the expected type before the
     * key is used in any documents.
     *
     * @param store Index store
     * @param key New key to register
     * @param information Information on the key to register
     * @param tx enclosing transaction
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException;

    /**
     * This method registers a new key for the specified index store with the given data type. This allows the IndexProvider
     * to prepare the index if necessary.
     *
     * It is expected that this method is first called with each new key to inform the index of the expected type before the
     * key is used in any documents.
     *
     * @param store Index store
     * @param key New key to register
     * @param information Information on the key to register
     * @param tx enclosing transaction
     * @param aliases es索引的别名
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void register(String store, String key, KeyInformation information, BaseTransaction tx, Set<String> aliases) throws BackendException;

    /**
     * This method registers a new key for the specified index store with the given data type. This allows the IndexProvider
     * to prepare the index if necessary.
     *
     * It is expected that this method is first called with each new key to inform the index of the expected type before the
     * key is used in any documents.
     *
     * @param store Index store
     * @param key New key to register
     * @param information Information on the key to register
     * @param tx enclosing transaction
     * @param aliases es索引的别名
     * @param settings 创建索引时指定的别名
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void register(String store, String key, KeyInformation information, BaseTransaction tx,Map<String,Object> settings, Set<String> aliases) throws BackendException;

    /**
     * 批量添加索引字段
     * @param store
     * @param keys
     * @param informations
     * @param tx
     * @param settings
     * @param aliases
     * @throws BackendException
     */
    void register(String store, List<String> keys, List<KeyInformation> informations, BaseTransaction tx,Map<String,Object> settings, Set<String> aliases) throws BackendException;

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                  all changes for each document in an {@link IndexMutation}.
     * @param information Information on the keys used in the mutation accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @throws org.janusgraph.diskstorage.BackendException
     * @see IndexMutation
     */
    void mutate(Map<String,Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Restores the index to the state of the primary data store as given in the {@code documents} variable. When this method returns, the index records
     * for the given documents exactly matches the provided data. Unlike {@link #mutate(java.util.Map, KeyInformation.IndexRetriever, BaseTransaction)}
     * this method does not do a delta-update, but entirely replaces the documents with the provided data or deletes them if the document content is empty.
     *
     * @param documents The outer map maps stores to documents, the inner contains the documents mapping document ids to the document content which is a
     *                  list of {@link IndexEntry}. If that list is empty, that means this document should not exist and ought to be deleted.
     * @param information Information on the keys used in the mutation accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @throws BackendException
     */
    void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    Long queryCount(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Executes the given query against the index.
     *
     * @param query Query to execute
     * @param information Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return The ids of all matching documents
     * @throws org.janusgraph.diskstorage.BackendException
     * @see IndexQuery
     */
    Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Executes the given raw query against the index
     *
     * @param query Query to execute
     * @param information Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return Results objects for all matching documents (i.e. document id and score)
     * @throws org.janusgraph.diskstorage.BackendException
     * @see RawQuery
     */
    Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * 删除所以数据
     * @param index
     * @throws org.janusgraph.diskstorage.BackendException
     * @param id
     */
    void deleteDocument(String index,String ... ids) throws BackendException;

    /**
     * Executes the given raw query against the index and returns the total hits. e.g. limit=0
     *
     * @param query Query to execute
     * @param information Information on the keys used in the query accessible through {@link KeyInformation.IndexRetriever}.
     * @param tx Enclosing transaction
     * @return Long total hits for query
     * @throws org.janusgraph.diskstorage.BackendException
     * @see RawQuery
     */
    Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Returns a transaction handle for a new index transaction.
     *
     * @return New Transaction Handle
     */
    BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the index
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void close() throws BackendException;

    /**
     * Clears the index and removes all entries in all stores.
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void clearStorage() throws BackendException;

    /**
     * Checks whether the index exists.
     * @return Flag indicating whether index exists
     * @throws org.janusgraph.diskstorage.BackendException
     */
    boolean exists() throws BackendException;

}

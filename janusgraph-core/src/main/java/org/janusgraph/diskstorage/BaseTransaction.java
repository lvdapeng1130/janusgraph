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

package org.janusgraph.diskstorage;

import java.util.Set;

/**
 * Represents a transaction for a particular storage backend.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface BaseTransaction {

    /**
     * Commits the transaction and persists all modifications to the backend.
     * 
     * Call either this method or {@link #rollback()} at most once per instance.
     *
     * @throws BackendException
     */
    void commit() throws BackendException;

    /**
     * Aborts (or rolls back) the transaction.
     * 
     * Call either this method or {@link #commit()} at most once per instance.
     *
     * @throws BackendException
     */
    void rollback() throws BackendException;

    default boolean isIndexMode(){
        return false;
    }

    /**
     * skip index type
     * @return
     */
    Set<String> getSkipIndexes();

}

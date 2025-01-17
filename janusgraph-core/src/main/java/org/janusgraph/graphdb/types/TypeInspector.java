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

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    default PropertyKey getExistingPropertyKey(String id) {
        return (PropertyKey)getExistingRelationType(id);
    }

    default EdgeLabel getExistingEdgeLabel(String id) {
        return (EdgeLabel)getExistingRelationType(id);
    }

    RelationType getExistingRelationType(String id);

    VertexLabel getExistingVertexLabel(String id);

    boolean containsRelationType(String name);

    RelationType getRelationType(String name);

}

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
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.util.encoding.LongEncoding;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    public static final String TOSTRING_DELIMITER = "-";

    private final String outVertexId;
    private final String typeId;
    private final String relationId;
    private final String inVertexId;

    private RelationIdentifier() {
        outVertexId = "";
        typeId = "";
        relationId = "";
        inVertexId = "";
    }

    public RelationIdentifier(final String outVertexId, final String typeId, final String relationId, final String inVertexId) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
    }

    public String getRelationId() {
        return relationId;
    }

    public String getTypeId() {
        return typeId;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        Preconditions.checkState(StringUtils.isNotBlank(inVertexId));
        return inVertexId;
    }

    public static RelationIdentifier get(String[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
       /* for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }*/
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : "");
    }

    /*public static RelationIdentifier get(int[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }
*/
    public String[] getLongRepresentation() {
        String[] r = new String[3 + (StringUtils.isNotBlank(inVertexId) ? 1 : 0)];
        r[0] = relationId;
        r[1] = outVertexId;
        r[2] = typeId;
        if (StringUtils.isNotBlank(inVertexId)) r[3] = inVertexId;
        return r;
    }

    @Override
    public int hashCode() {
        return relationId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        RelationIdentifier oth = (RelationIdentifier) other;
        return relationId.equals(oth.relationId) && typeId.equals(oth.typeId);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER).append(LongEncoding.encode(outVertexId))
                .append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
        if (StringUtils.isNotBlank(inVertexId)){
            s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(inVertexId));
        }
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        try {
            return new RelationIdentifier(LongEncoding.decode(elements[1]),
                    LongEncoding.decode(elements[2]),
                    LongEncoding.decode(elements[0]),
                    elements.length == 4 ? LongEncoding.decode(elements[3]) : "");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }
}

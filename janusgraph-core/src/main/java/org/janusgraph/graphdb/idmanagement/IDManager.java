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

package org.janusgraph.graphdb.idmanagement;


import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.core.InvalidIDException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

/**
 * Handles the allocation of ids based on the type of element
 * Responsible for the bit-wise pattern of JanusGraph's internal id scheme.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IDManager {

    /**
     *bit mask- Description (+ indicates defined type, * indicates proper &amp; defined type)
     *
     *      0 - + User created Vertex
     *    000 -     * Normal vertices
     *    010 -     * Partitioned vertices
     *    100 -     * Unmodifiable (e.g. TTL'ed) vertices
     *    110 -     + Reserved for additional vertex type
     *      1 - + Invisible
     *     11 -     * Invisible (user created/triggered) Vertex [for later]
     *     01 -     + Schema related vertices
     *    101 -         + Schema Type vertices
     *   0101 -             + Relation Type vertices
     *  00101 -                 + Property Key
     * 000101 -                     * User Property Key
     * 100101 -                     * System Property Key
     *  10101 -                 + Edge Label
     * 010101 -                     * User Edge Label
     * 110101 -                     * System Edge Label
     *   1101 -             Other Type vertices
     *  01101 -                 * Vertex Label
     *    001 -         Non-Type vertices
     *   1001 -             * Generic Schema Vertex
     *   0001 -             Reserved for future
     *
     *
     */
    public enum VertexIDType {
        UserVertex {
            @Override
            final long offset() {
                return 1L;
            }

            @Override
            final String suffix() {
                //return 0L;
                return "0";
            } // 0b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        NormalVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final String suffix() {
                //return 0L;
                return "000";
            } // 000b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        PartitionedVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final String suffix() {
                //return 2L;
                return "010";
            } // 010b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        UnmodifiableVertex {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final String suffix() {
                //return 4L;
                return "100";
            } // 100b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        Invisible {
            @Override
            final long offset() {
                return 1L;
            }

            @Override
            final String suffix() {
                //return 1L;
                return "1";
            } // 1b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        InvisibleVertex {
            @Override
            final long offset() {
                return 2L;
            }

            @Override
            final String suffix() {
                //return 3L;
                return "11";
            } // 11b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        Schema {
            @Override
            final long offset() {
                return 2L;
            }

            @Override
            final String suffix() {
                //return 1L;
                return "01";
            } // 01b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        SchemaType {
            @Override
            final long offset() {
                return 3L;
            }

            @Override
            final String suffix() {
                //return 5L;
                return "101";
            } // 101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        RelationType {
            @Override
            final long offset() {
                return 4L;
            }

            @Override
            final String suffix() {
                //return 5L;
                return "0101";
            } // 0101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        PropertyKey {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final String suffix() {
                //return 5L;
                return "00101";
            }    // 00101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserPropertyKey {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final String suffix() {
                //return 5L;
                return "000101";
            }    // 000101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemPropertyKey {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final String suffix() {
                //return 37L;
                return "100101";
            }    // 100101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        EdgeLabel {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final String suffix() {
                //return 21L;
                return "10101";
            } // 10101b

            @Override
            final boolean isProper() {
                return false;
            }
        },
        UserEdgeLabel {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final String suffix() {
                //return 21L;
                return "010101";
            } // 010101b

            @Override
            final boolean isProper() {
                return true;
            }
        },
        SystemEdgeLabel {
            @Override
            final long offset() {
                return 6L;
            }

            @Override
            final String suffix() {
                //return 53L;
                return "110101";
            } // 110101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        VertexLabel {
            @Override
            final long offset() {
                return 5L;
            }

            @Override
            final String suffix() {
                //return 13L;
                return "01101";
            }    // 01101b

            @Override
            final boolean isProper() {
                return true;
            }
        },

        GenericSchemaType {
            @Override
            final long offset() {
                return 4L;
            }

            @Override
            final String suffix() {
                //return 9L;
                return "1001";
            }    // 1001b

            @Override
            final boolean isProper() {
                return true;
            }
        };

        abstract long offset();

        abstract String suffix();

        abstract boolean isProper();

        /**
         * (count << offset()) 把count向左移位offset()位，高位舍去，空出的低位使用0补位
         * (count << offset()) | suffix()
         * @param count
         * @return
         */
        public final String addPadding(String count) {
            assert offset()>0;
            //Preconditions.checkArgument(count>0 && count<(1L <<(TOTAL_BITS-offset())),"Count out of range for type [%s]: %s",this,count);
            //return (count << offset()) | suffix();
            return count+suffix();
        }

        public final String removePadding(String id) {
            //return id >>> offset();
            return id.substring(0,id.length()-(int)offset());
        }

        /**
         * 取低`offset()`位和`suffix()`进行比较
         * byte->: 11110101 & 00000111 =00000101
         * (id & ((1L << offset()) - 1)) 取id低位`offset()`位。
         * @param id
         * @return
         */
        public final boolean is(String id) {
            int start=id.length()-(int)offset();
            int end=start+(int) offset();
            String suffix = id.substring(start, end);
            return suffix().equals(suffix);
           // return (id & ((1L << offset()) - 1)) == suffix();
        }

        public final boolean isSubType(VertexIDType type) {
            return is(type.suffix());
        }
    }

    /**
     * Id of the partition that schema elements are assigned to
     * 分配架构元素的分区的ID
     */
    public static final int SCHEMA_PARTITION = 0;

    public static final int PARTITIONED_VERTEX_PARTITION = 1;


    /**
     * Number of bits that need to be reserved from the type ids for storing additional information during serialization
     * 需要在类型ID中保留的位数，以在序列化期间存储其他信息
     */
    public static final int TYPE_LEN_RESERVE = 3;

    /**
     * Total number of bits available to a JanusGraph assigned id
     * We use only 63 bits to make sure that all ids are positive
     * JanusGraph分配的ID可用的位数,我们仅使用63位来确保所有ID均为正
     */
    private static final long TOTAL_BITS = Long.SIZE-1;

    /**
     * Maximum number of bits that can be used for the partition prefix of an id
     * 可用于ID的分区前缀的最大位数
     */
    private static final long MAX_PARTITION_BITS = 16;
    /**
     * Default number of bits used for the partition prefix. 0 means there is no partition prefix
     * 用于分区前缀的默认位数。 0表示没有分区前缀
     */
    private static final long DEFAULT_PARTITION_BITS = 0;
    /**
     * The padding bit width for user vertices
     * 用户顶点的填充位宽度
     */
    public static final long USERVERTEX_PADDING_BITWIDTH = VertexIDType.NormalVertex.offset();

    /**
     * The maximum number of padding bits of any type
     * 任何类型的最大填充位数
     */
    public static final long MAX_PADDING_BITWIDTH = VertexIDType.UserEdgeLabel.offset();

    /**
     * Bound on the maximum count for a schema id
     */
    private static final long SCHEMA_COUNT_BOUND = (1L << (TOTAL_BITS - MAX_PADDING_BITWIDTH - TYPE_LEN_RESERVE));


    private final long partitionBits;
    private final long partitionOffset;
    private final long partitionIDBound;

    private final long relationCountBound;
    private final long vertexCountBound;


    public IDManager(long partitionBits) {
        Preconditions.checkArgument(partitionBits >= 0);
        Preconditions.checkArgument(partitionBits <= MAX_PARTITION_BITS,
                "Partition bits can be at most %s bits", MAX_PARTITION_BITS);
        this.partitionBits = partitionBits;

        partitionIDBound = (1L << (partitionBits));

        relationCountBound = partitionBits==0?Long.MAX_VALUE:(1L << (TOTAL_BITS - partitionBits));
        assert VertexIDType.NormalVertex.offset()>0;
        vertexCountBound = (1L << (TOTAL_BITS - partitionBits - USERVERTEX_PADDING_BITWIDTH));


        partitionOffset = Long.SIZE - partitionBits;
    }

    public IDManager() {
        this(DEFAULT_PARTITION_BITS);
    }

    public long getPartitionBound() {
        return partitionIDBound;
    }

    /* ########################################################
                   User Relations and Vertices
       ########################################################  */

     /*		--- JanusGraphElement id bit format ---
      *  [ 0 | count | partition | ID padding (if any) ]
     */

    private String constructId(String count, long partition, VertexIDType type) {
        /*Preconditions.checkArgument(partition<partitionIDBound && partition>=0,"Invalid partition: %s",partition);
        Preconditions.checkArgument(count>=0);
        Preconditions.checkArgument(VariableLong.unsignedBitLength(count)+partitionBits+
                (type==null?0:type.offset())<=TOTAL_BITS);
        Preconditions.checkArgument(type==null || type.isProper());
        long id = (count<<partitionBits)+partition;
        if (type!=null) id = type.addPadding(id);
        return id;*/
        String id=count;
        String newid=id+"_"+partition;
        if (type!=null){
            newid=newid+"_";
            newid = type.addPadding(newid);
        }
        return newid;

    }

    private static VertexIDType getUserVertexIDType(String vertexId) {
        VertexIDType type=null;
        if (VertexIDType.NormalVertex.is(vertexId)) type=VertexIDType.NormalVertex;
        else if (VertexIDType.PartitionedVertex.is(vertexId)) type=VertexIDType.PartitionedVertex;
        else if (VertexIDType.UnmodifiableVertex.is(vertexId)) type=VertexIDType.UnmodifiableVertex;
        if (null == type) {
            throw new InvalidIDException("Vertex ID " + vertexId + " has unrecognized type");
        }
        return type;
    }

    public final boolean isUserVertexId(String vertexId) {
        return (VertexIDType.NormalVertex.is(vertexId) || VertexIDType.PartitionedVertex.is(vertexId) || VertexIDType.UnmodifiableVertex.is(vertexId));
    }

    public long getPartitionId(String vertexId) {
        if (VertexIDType.Schema.is(vertexId)) return SCHEMA_PARTITION;
        /*assert isUserVertexId(vertexId) && getUserVertexIDType(vertexId)!=null;
        long partition = (vertexId>>>USERVERTEX_PADDING_BITWIDTH) & (partitionIDBound-1);
        assert partition>=0;*/
        //long partition=Math.abs(vertexId.hashCode())%partitionIDBound;
        String[] strings = vertexId.split("_");
        if(strings!=null&&strings.length>=2){
            long partition = Long.parseLong(strings[1]);
            return partition;
        }else {
            long partition = Math.abs(vertexId.hashCode()) % partitionIDBound;
            return partition;
        }
    }

    public StaticBuffer getKey(String vertexId) {
        if (VertexIDType.Schema.is(vertexId)) {
            //No partition for schema vertices
            return StaticArrayBuffer.of(vertexId.getBytes());
            //return BufferUtil.getLongBuffer(vertexId);
        } else {
            assert isUserVertexId(vertexId);
            VertexIDType type = getUserVertexIDType(vertexId);
            assert type.offset()==USERVERTEX_PADDING_BITWIDTH;
            /*long partition = getPartitionId(vertexId);
            long count = vertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
            assert count>0;
            long keyId = (partition<<partitionOffset) | type.addPadding(count);
            return BufferUtil.getLongBuffer(keyId);*/
            //String newId=vertexId+type.suffix();
            return StaticArrayBuffer.of(vertexId.getBytes());
        }
    }

    public String getKeyID(StaticBuffer b) {
        //String value=BufferUtil.getSerializer().readObjectNotNull(b.asReadBuffer(),String.class);
        String value=new String(b.asByteBuffer().array());
        if (VertexIDType.Schema.is(value)) {
            return value;
        } else {
            //VertexIDType type = getUserVertexIDType(value);
            //long partition = partitionOffset<Long.SIZE?value>>>partitionOffset:0;
            //long count = (value>>>USERVERTEX_PADDING_BITWIDTH) & ((1L <<(partitionOffset-USERVERTEX_PADDING_BITWIDTH))-1);
            //long partition =this.getPartitionId(value);
            //return constructId(value,partition,type);
            return value;
        }
    }

    public String getRelationID(String count, long partition) {
        //Preconditions.checkArgument(count>0 && count< relationCountBound,"Invalid count for bound: %s", relationCountBound);
        //return constructId(count, partition, null);
        return constructId(count, partition, null);
    }


    public String getVertexID(String count, long partition, VertexIDType vertexType) {
        Preconditions.checkArgument(VertexIDType.UserVertex.is(vertexType.suffix()),"Not a user vertex type: %s",vertexType);
        //Preconditions.checkArgument(count>0 && count<vertexCountBound,"Invalid count for bound: %s", vertexCountBound);
        if (vertexType==VertexIDType.PartitionedVertex) {
            Preconditions.checkArgument(partition==PARTITIONED_VERTEX_PARTITION);
            return getCanonicalVertexIdFromCount(count);
        } else {
            return constructId(count, partition, vertexType);
        }
    }

    public long getPartitionHashForId(String id) {
        /*Preconditions.checkArgument(id>0);
        Preconditions.checkState(partitionBits>0, "no partition bits");
        long result = 0;
        int offset = 0;
        while (offset<Long.SIZE) {
            result = result ^ ((id>>>offset) & (partitionIDBound-1));
            offset+=partitionBits;
        }
        assert result>=0 && result<partitionIDBound;
        return result;*/
        String[] strings = id.split("_");
        if(strings!=null&&strings.length>=2){
            long partition = Long.parseLong(strings[1]);
            return partition;
        }else {
            long partition = Math.abs(id.hashCode()) % partitionIDBound;
            return partition;
        }
    }

    public long getHashPartition(long hash){
        long partition=Math.abs(hash)%partitionIDBound;
        return partition;
    }

    private String getCanonicalVertexIdFromCount(String count) {
        long partition = getPartitionHashForId(count);
        return constructId(count,partition,VertexIDType.PartitionedVertex);
    }

    public String getCanonicalVertexId(String partitionedVertexId) {
        Preconditions.checkArgument(VertexIDType.PartitionedVertex.is(partitionedVertexId));
        //long count = partitionedVertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
        return getCanonicalVertexIdFromCount(partitionedVertexId);
    }

    public boolean isCanonicalVertexId(String partitionVertexId) {
        return partitionVertexId==getCanonicalVertexId(partitionVertexId);
    }

    public String getPartitionedVertexId(String partitionedVertexId, long otherPartition) {
        Preconditions.checkArgument(VertexIDType.PartitionedVertex.is(partitionedVertexId));
        //long count = partitionedVertexId>>>(partitionBits+USERVERTEX_PADDING_BITWIDTH);
        //assert count>0;
        return constructId(partitionedVertexId,otherPartition,VertexIDType.PartitionedVertex);
    }

    public String[] getPartitionedVertexRepresentatives(String partitionedVertexId) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertexId));
        assert getPartitionBound()<Integer.MAX_VALUE;
        String[] ids = new String[(int)getPartitionBound()];
        for (int i=0;i<getPartitionBound();i++) {
            ids[i]=getPartitionedVertexId(partitionedVertexId,i);
        }
        return ids;
    }

    /**
     * Converts a user provided long id into a JanusGraph vertex id. The id must be positive and less than {@link #getVertexCountBound()}.
     * This method is useful when providing ids during vertex creation via {@link org.apache.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param id String id
     * @return a corresponding JanusGraph vertex id
     * @see #fromVertexId(String)
     */
    public String toVertexId(String id) {
        Preconditions.checkArgument(StringUtils.isNotBlank(id), "Vertex id must be positive: %s", id);
        //Preconditions.checkArgument(vertexCountBound > id, "Vertex id is too large: %s", id);
        long partition=this.getHashPartition(id.hashCode());
        return getVertexID(id,partition,IDManager.VertexIDType.NormalVertex);
        //return id<<(partitionBits+USERVERTEX_PADDING_BITWIDTH);
    }

    /**
     * Converts a JanusGraph vertex id to the user provided id as the inverse mapping of {@link #toVertexId(String)}.
     *
     * @param id JanusGraph vertex id (must be positive)
     * @return original user provided id
     * @see #toVertexId(String)
     */
    public String fromVertexId(String id) {
        /*Preconditions.checkArgument(id >>> USERVERTEX_PADDING_BITWIDTH+partitionBits > 0
            && id <= (vertexCountBound-1)<<USERVERTEX_PADDING_BITWIDTH+partitionBits, "Invalid vertex id provided: %s", id);*/
        //return id>>USERVERTEX_PADDING_BITWIDTH+partitionBits;
        //return VertexIDType.NormalVertex.removePadding(id);
        String[] strings = id.split("_");
        if(strings!=null&&strings.length>=1){
            return strings[0];
        }else {
            return VertexIDType.NormalVertex.removePadding(id);
        }
    }

    public boolean isPartitionedVertex(String id) {
        return isUserVertexId(id) && VertexIDType.PartitionedVertex.is(id);
    }

    public long getRelationCountBound() {
        return relationCountBound;
    }

    public long getVertexCountBound() {
        return vertexCountBound;
    }

    /*

    Temporary ids are negative and don't have partitions

     */

    public static String getTemporaryRelationID(String count) {
        return makeTemporary(count);
    }

    public static String getTemporaryVertexID(VertexIDType type, String count) {
        Preconditions.checkArgument(type.isProper(),"Invalid vertex id type: %s",type);
        return makeTemporary(type.addPadding(count));
    }

    private static String makeTemporary(String id) {
        //Preconditions.checkArgument(id>0);
       // return (1L <<63) | id; //make negative but preserve bit pattern
        return id;
    }

    public static boolean isTemporary(String id) {
        return StringUtils.isBlank(id);
    }

    /* ########################################################
               Schema Vertices
   ########################################################  */

    /* --- JanusGraphRelation Type id bit format ---
      *  [ 0 | count | ID padding ]
      *  (there is no partition)
     */


    private static void checkSchemaTypeId(VertexIDType type, String count) {
        Preconditions.checkArgument(VertexIDType.Schema.is(type.suffix()),"Expected schema vertex but got: %s",type);
        Preconditions.checkArgument(type.isProper(),"Expected proper type but got: %s",type);
       /* Preconditions.checkArgument(count > 0 && count < SCHEMA_COUNT_BOUND,
                "Invalid id [%s] for type [%s] bound: %s", count, type, SCHEMA_COUNT_BOUND);*/
    }

    public static String getSchemaId(VertexIDType type, String count) {
        checkSchemaTypeId(type,count);
        return type.addPadding(count);
    }

    private static boolean isProperRelationType(String id) {
        return VertexIDType.UserEdgeLabel.is(id) || VertexIDType.SystemEdgeLabel.is(id)
                || VertexIDType.UserPropertyKey.is(id) || VertexIDType.SystemPropertyKey.is(id);
    }

    public static String stripEntireRelationTypePadding(String id) {
        Preconditions.checkArgument(isProperRelationType(id));
        return VertexIDType.UserEdgeLabel.removePadding(id);
    }

    public static String stripRelationTypePadding(String id) {
        Preconditions.checkArgument(isProperRelationType(id));
        return VertexIDType.RelationType.removePadding(id);
    }

    public static String addRelationTypePadding(String id) {
        String typeId = VertexIDType.RelationType.addPadding(id);
        Preconditions.checkArgument(isProperRelationType(typeId));
        return typeId;
    }

    public static boolean isSystemRelationTypeId(String id) {
        return VertexIDType.SystemEdgeLabel.is(id) || VertexIDType.SystemPropertyKey.is(id);
    }

    public static long getSchemaCountBound() {
        return SCHEMA_COUNT_BOUND;
    }

    //ID inspection ------------------------------

    public final boolean isSchemaVertexId(String id) {
        return isRelationTypeId(id) || isVertexLabelVertexId(id) || isGenericSchemaVertexId(id);
    }

    public final boolean isRelationTypeId(String id) {
        return VertexIDType.RelationType.is(id);
    }

    public final boolean isEdgeLabelId(String id) {
        return VertexIDType.EdgeLabel.is(id);
    }

    public final boolean isPropertyKeyId(String id) {
        return VertexIDType.PropertyKey.is(id);
    }

    public boolean isGenericSchemaVertexId(String id) {
        return VertexIDType.GenericSchemaType.is(id);
    }

    public boolean isVertexLabelVertexId(String id) {
        return VertexIDType.VertexLabel.is(id);
    }

    public boolean isUnmodifiableVertex(String id) {
        return isUserVertexId(id) && VertexIDType.UnmodifiableVertex.is(id);
    }

//    public boolean isPartitionedVertex(long id) {
//        return IDManager.this.isPartitionedVertex(id);
//    }
//
//    public long getCanonicalVertexId(long partitionedVertexId) {
//        return IDManager.this.getCanonicalVertexId(partitionedVertexId);
//    }

//    /* ########################################################
//               Inspector
//   ########################################################  */
//
//
//    private final IDInspector inspector = new IDInspector() {
//
//        @Override
//        public final boolean isSchemaVertexId(long id) {
//            return isRelationTypeId(id) || isVertexLabelVertexId(id) || isGenericSchemaVertexId(id);
//        }
//
//        @Override
//        public final boolean isRelationTypeId(long id) {
//            return VertexIDType.RelationType.is(id);
//        }
//
//        @Override
//        public final boolean isEdgeLabelId(long id) {
//            return VertexIDType.EdgeLabel.is(id);
//        }
//
//        @Override
//        public final boolean isPropertyKeyId(long id) {
//            return VertexIDType.PropertyKey.is(id);
//        }
//
//        @Override
//        public boolean isSystemRelationTypeId(long id) {
//            return IDManager.isSystemRelationTypeId(id);
//        }
//
//        @Override
//        public boolean isGenericSchemaVertexId(long id) {
//            return VertexIDType.GenericSchemaType.is(id);
//        }
//
//        @Override
//        public boolean isVertexLabelVertexId(long id) {
//            return VertexIDType.VertexLabel.is(id);
//        }
//
//
//
//        @Override
//        public final boolean isUserVertexId(long id) {
//            return IDManager.this.isUserVertex(id);
//        }
//
//        @Override
//        public boolean isUnmodifiableVertex(long id) {
//            return isUserVertex(id) && VertexIDType.UnmodifiableVertex.is(id);
//        }
//
//        @Override
//        public boolean isPartitionedVertex(long id) {
//            return IDManager.this.isPartitionedVertex(id);
//        }
//
//        @Override
//        public long getCanonicalVertexId(long partitionedVertexId) {
//            return IDManager.this.getCanonicalVertexId(partitionedVertexId);
//        }
//
//    };
//
//    public IDInspector getIdInspector() {
//        return inspector;
//    }

}

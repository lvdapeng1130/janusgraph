package org.janusgraph.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class TransactionCustom implements Serializable {
    /**
     * 针对在图库管理平台新增schema新增后在Nifi抽取没有刷新schema的问题。
     */
    private boolean refreshSchemaCache=false;
}

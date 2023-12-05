package org.janusgraph.kggraph.bo;

import lombok.Data;

import java.util.Map;

@Data
public class GraphEdge {
    /*************graphvis的边参数(http://www.graphvis.cn/article/9)*************/
    private String id;
    private String type;
    private String label;
    private String source;
    private String target;
    private Map<String,String> properties;
    /****************自定义参数*************/
    private String typeName;

}

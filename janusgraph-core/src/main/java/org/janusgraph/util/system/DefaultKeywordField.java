package org.janusgraph.util.system;

public enum DefaultKeywordField {
    TID("tid"),LINK_TID("link_tid"),LINK_TYPE("link_type"),LINK_ROLE("link_role")
    ,LEFT_TID("left_tid"),LEFT_TYPE("left_type"),RIGHT_TID("right_tid")
    ,RIGHT_TYPE("right_type");
    private String name;

    public String getName() {
        return name;
    }

    DefaultKeywordField(String name) {
        this.name = name;
    }

    public static boolean isKeyWordField(String field){
        DefaultKeywordField[] values = DefaultKeywordField.values();
        for(DefaultKeywordField defultKeywordField:values){
            if(defultKeywordField.getName().equalsIgnoreCase(field))
            {
                return true;
            }
        }
        return false;
    }

}

package org.janusgraph.util.system;

import java.util.HashSet;
import java.util.Set;

public enum DefaultKeywordField {
    TID("tid"),LINK_TID("link_tid"),LINK_TYPE("link_type"),LINK_ROLE("link_role")
    ,LEFT_TID("left_tid"),LEFT_TYPE("left_type"),RIGHT_TID("right_tid")
    ,RIGHT_TYPE("right_type");
    private static Set<String> defaultFields=new HashSet<>();
    private static Set<String> defaultNotSaveFields=new HashSet<>();
    static {
        DefaultKeywordField[] values = DefaultKeywordField.values();
        for(DefaultKeywordField defultKeywordField:values){
            defaultFields.add(defultKeywordField.getName());
        }
        defaultNotSaveFields.add(DefaultKeywordField.TID.getName());
        defaultNotSaveFields.add(DefaultKeywordField.LINK_TID.getName());
        defaultNotSaveFields.add(DefaultKeywordField.LEFT_TID.getName());
        defaultNotSaveFields.add(DefaultKeywordField.RIGHT_TID.getName());
        defaultNotSaveFields.add(DefaultKeywordField.LINK_TYPE.getName());
        defaultNotSaveFields.add(DefaultKeywordField.LINK_ROLE.getName());
    }
    private String name;

    public String getName() {
        return name;
    }

    DefaultKeywordField(String name) {
        this.name = name;
    }

    public static boolean isKeyWordField(String field){
       return defaultFields.contains(field);
    }

    public static boolean isNotSavedField(String field){
       return defaultNotSaveFields.contains(field);
    }

}

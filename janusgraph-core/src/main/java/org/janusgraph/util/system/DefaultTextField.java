package org.janusgraph.util.system;

import java.util.HashSet;
import java.util.Set;

public enum DefaultTextField {
    TITLE("title"),LINK_TEXT("link_text");
    private static Set<String> defaultFields=new HashSet<>();
    static {
        DefaultTextField[] values = DefaultTextField.values();
        for(DefaultTextField defaultTextField:values){
            defaultFields.add(defaultTextField.getName());
        }
    }
    private String name;

    public String getName() {
        return name;
    }

    DefaultTextField(String name) {
        this.name = name;
    }

    public static boolean isKeyWordField(String field){
        return defaultFields.contains(field);
    }
}

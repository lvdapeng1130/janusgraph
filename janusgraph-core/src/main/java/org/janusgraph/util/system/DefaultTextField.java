package org.janusgraph.util.system;

public enum DefaultTextField {
    TITLE("title"),LINK_TEXT("link_text");
    private String name;

    public String getName() {
        return name;
    }

    DefaultTextField(String name) {
        this.name = name;
    }

    public static boolean isKeyWordField(String field){
        DefaultTextField[] values = DefaultTextField.values();
        for(DefaultTextField defultTextField:values){
            if(defultTextField.getName().equalsIgnoreCase(field))
            {
                return true;
            }
        }
        return false;
    }
}

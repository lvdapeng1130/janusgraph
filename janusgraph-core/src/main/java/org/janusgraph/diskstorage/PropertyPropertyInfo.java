package org.janusgraph.diskstorage;

import lombok.Data;
import org.janusgraph.core.PropertyKey;

@Data
public class PropertyPropertyInfo {
    //属性类型ID
    private String propertyTypeId;
    //属性的值md5
    private String propertyValue_md5;
    //属性的属性类型ID
    private String propertyPropertyKeyId;
    //属性的属性值
    private Object propertyPropertyValue;
    //属性值的ID
    private String propertyId;
    //属性的属性类型
    private PropertyKey key;

    public PropertyPropertyInfo(String propertyTypeId, String propertyValue_md5, String propertyPropertyKeyId, Object propertyPropertyValue, String propertyId, PropertyKey key) {
        this.propertyTypeId = propertyTypeId;
        this.propertyValue_md5 = propertyValue_md5;
        this.propertyPropertyKeyId = propertyPropertyKeyId;
        this.propertyPropertyValue = propertyPropertyValue;
        this.propertyId = propertyId;
        this.key = key;
    }
}

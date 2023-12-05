package org.janusgraph.diskstorage;

public class PropertyEntry {
    private final Iterable<Entry> properties;
    private Iterable<Entry> propertyProperties;

    private Iterable<Entry> medias;

    private Iterable<Entry> notes;

    public PropertyEntry(Iterable<Entry> properties) {
        this.properties = properties;
    }

    public Iterable<Entry> getProperties() {
        return properties;
    }

    public Iterable<Entry> getPropertyProperties() {
        return propertyProperties;
    }

    public void setPropertyProperties(Iterable<Entry> propertyProperties) {
        this.propertyProperties = propertyProperties;
    }

    public Iterable<Entry> getMedias() {
        return medias;
    }

    public void setMedias(Iterable<Entry> medias) {
        this.medias = medias;
    }

    public Iterable<Entry> getNotes() {
        return notes;
    }

    public void setNotes(Iterable<Entry> notes) {
        this.notes = notes;
    }
}

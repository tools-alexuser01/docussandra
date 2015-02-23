package com.strategicgains.docussandra.bucketmanagement;

/**
 *
 * @author udeyoje
 */
public class Column<N, V> {

    private N name;
    private V value;

    public Column(N name, V value) {
        this.name = name;
        this.value = value;
    }

    public N getName() {
        return name;
    }

    public V getValue() {
        return value;
    }
    
    

}

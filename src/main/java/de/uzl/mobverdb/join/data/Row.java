package de.uzl.mobverdb.join.data;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Models a single row. Each line is assumed to have a join key in the first column.
 * 
 * @author Martin Thurau
 */
public class Row implements Serializable {
    
    /** generated */
    private static final long serialVersionUID = -2376798714947561859L;
    private final Integer key;
    private final List<String> data;

    /**
     * Create a new row from a given Iterable. The first item must contain a valid
     * integer, as it will be casted to Integer. If not, a NumberFormatException will be thrown.
     */
    public Row(Iterable<String> data) {
        this.key = Integer.parseInt(Iterables.get(data, 0));
        this.data = Lists.newArrayList(Iterables.skip(data, 1));
    }
    
    /**
     * Create a new row with a given key with given data.
     */
    public Row(Integer key, Iterable<String> data) {
        this.key = key;
        this.data = Lists.newArrayList(data);
    }
    
    /** 
     * @return the first column as int
     */
    public Integer getKey() {
        return this.key;
    }
    
    /**
     * @return all column (except the first)
     */
    public List<String> getData() {
        return this.data;
    }
}

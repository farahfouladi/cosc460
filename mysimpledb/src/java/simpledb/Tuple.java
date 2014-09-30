package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc desc;
    private RecordId rId;
    private Field[] fields;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	if (td.numFields() > 0) {
    		this.desc = td;
    		fields = new Field[td.numFields()];
    	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return this.rId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < this.desc.numFields()) {
        	if (f instanceof IntField && this.desc.getFieldType(i).getLen()==4) {
        		fields[i] = f;
        	}
        	else if (f instanceof StringField && this.desc.getFieldType(i).getLen()>4) {
        		fields[i] = f;
        	}
        	else {
        		throw new RuntimeException();
        	}
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i < this.desc.numFields()) {
        	return fields[i];
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
        int i;
        int n = this.desc.numFields();
        String s = "";
        for (i=0;i<n;i++) {
        	if (i<n-1) {
        		s += fields[i]+"\t";
        	}
        	else  {
        		s+=fields[i];
        	}
        }
        return s;
    }
    
    public static Tuple merge(Tuple t1, Tuple t2, TupleDesc td) {
    	int size = td.numFields();
    	Tuple tup = new Tuple(td);
    	int i;
    	int index1 = 0;
    	int index2 = 0;
    	for (i=0;i<size;i++) {
        	if (i<t1.getTupleDesc().numFields()) {
        		Field f = t1.getField(index1);
        		tup.setField(i, f);
        		index1++;
        	}
        	else {
        		Field f = t2.getField(index2);
        		tup.setField(i, f);
        		index2++;
        	}
    	}
    	return tup;
    }

}

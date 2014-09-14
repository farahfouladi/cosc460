package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        
        public String getName() {
        	return this.fieldName;
        }
        
        public Type getType() {
        	return this.fieldType;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    private TDItem[] items;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	System.out.println("making tuple desc");
        int num = typeAr.length;
        System.out.println("TD: num = " + num);
        TDItem item;
        if (num > 0) {
        	items = new TDItem[num];
        	for (int i=0;i<num;i++) {
        		item = new TDItem(typeAr[i], fieldAr[i]);
        		items[i] = item;
        	}
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int num = 0;
        num = typeAr.length;
        TDItem item;
        if (num > 0) {
        	items = new TDItem[num];
        	for (int i=0;i<num;i++) {
        		item = new TDItem(typeAr[i], null);
        		items[i] = item;
        	}
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        String name = null;
        if (this.items == null) {
        	return null;
        }
    	try {
        	name = this.items[i].getName();
        }
        catch (IndexOutOfBoundsException e) {
        	throw new NoSuchElementException();
        }
    	
    	return name;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        Type t = null;
        try {
        	t = this.items[i].getType();
        }
        catch (IndexOutOfBoundsException e) {
        	throw new NoSuchElementException();
        }
        return t;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int k;
        int index = 0;
        boolean flag = false;
        int num = this.numFields();
        if (name == null) {
        	throw new NoSuchElementException();
        }
        if (this.items == null) {
        	throw new NoSuchElementException();
        }
        for (k=0;k<num;k++) {
        	if (this.items[k].getName() != null && name.equals(this.items[k].getName())) {
        		index = k;
        		flag = true;
        		break;
        	}
        }
        if (!flag) {
        	throw new NoSuchElementException();
        }
		return index;

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int i;
        int size = 0;
        int num = this.numFields();
        for (i=0;i<num;i++) {
        	size += this.items[i].getType().getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newSize = td1.numFields() + td2.numFields();
        Type[] types = new Type[newSize];
        String[] fieldNames = new String[newSize];
        int i;
        int index1 = 0;
        int index2 = 0;
        for (i=0;i<newSize;i++) {
        	if (i<td1.numFields()) {
        		types[i] = td1.getFieldType(index1);
        		fieldNames[i] = td1.getFieldName(index1);
        		index1++;
        	}
        	else {
        		types[i] = td2.getFieldType(index2);
        		fieldNames[i] = td2.getFieldName(index2);
        		index2++;
        	}
        }
        TupleDesc tupleMerge = new TupleDesc(types, fieldNames);
        return tupleMerge;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o==null) {
    		return false;
    	}
    	if (!(o instanceof TupleDesc)) {
    		return false;
    	}
        if (this.numFields() != ((TupleDesc) o).numFields()) {
        	return false;
        }
        int i;
        for (i=0;i<this.numFields();i++) {
        	if ( this.getFieldType(i) != ((TupleDesc) o).getFieldType(i)) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        int i;
        String toReturn = "";
        for (i=0;i<this.numFields();i++) {
        	toReturn += this.getFieldName(i)+"("+this.getFieldType(i)+")";
        	if (i<this.numFields()-1) {
        		toReturn += ", ";
        	}
        }
        return toReturn;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public static Iterator<TDItem> iterator() { 
    	//Iterator<TDItem> i = Arrays.asList(items).iterator();
        return null;
    }

}

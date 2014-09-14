package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator i;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
    	this.i = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    	this.i.open();
    	
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	System.out.println("Table Id = " + this.tableid);
    	DbFile file = Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc td = file.getTupleDesc();
    	String alias = getAlias();
    	int numTuples = td.numFields();
    	System.out.println("NUM FIELDS TAODAY " + numTuples);
    	System.out.println(numTuples);
    	int k;
    	Type[] types = new Type[numTuples];
    	String[] names = new String[numTuples];
    	for (k=0;k<numTuples;k++) {
    		System.out.println(names[k]);
    		types[k] = td.getFieldType(k);
    		names[k] = alias + "." + td.getFieldName(k);
    	}
    	TupleDesc newTd = new TupleDesc(types,names); 
        return newTd;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
    	if (i == null){
    		return false;
    	}
    	return i.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	if(i==null){
    		throw new NoSuchElementException();
    	}
        Tuple tup = i.next();
        if(tup == null){
        	throw new NoSuchElementException();
        }
        
        return tup;
    }

    public void close() {
    	this.i = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	this.i.close();
    	this.i.open();
    }
}

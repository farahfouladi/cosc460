package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private int times_called;
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        times_called = 0;
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        times_called = 0;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	int count = 0;
    	times_called++;
    	if (times_called > 1) { //if initially false, child is at end
    		return null;
    	}
    	while (child.hasNext()) {
    		Tuple tup = child.next();
    		count++;
    		BufferPool bp = Database.getBufferPool();
    		try {
				bp.insertTuple(t, tableid, tup);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    	Tuple tup_toReturn = new Tuple(td);
    	Field f = new IntField(count);
    	tup_toReturn.setField(0, f);
    	return tup_toReturn;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	// may need to change
        child = children[0];
    }
}

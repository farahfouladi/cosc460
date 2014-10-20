package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int times_called;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
        times_called = 0;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        times_called = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
    			bp.deleteTuple(t, tup);
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
        child = children[0];
    }

}

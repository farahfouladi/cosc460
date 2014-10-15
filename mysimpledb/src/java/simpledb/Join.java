package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private Tuple t1;
    private Tuple t2;
    private Tuple tup; // the new tuple
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.p = p;
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	//System.out.println("opeining join iterator!");
    	super.open();
    	child1.open();
    	if (child1.hasNext()) {
    		t1 = child1.next();
    		//System.out.println("Just set t1 in open to be: "+t1);
    	}
    	child2.open();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child1.rewind();
    	if (child1.hasNext()) {
    		t1 = child1.next();
    	}
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p/>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p/>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	//System.out.println("fetch next!");
    	Tuple t = null;
    	boolean flag = false;
    	while (child2.hasNext()) {
        	t2 = child2.next();
        	//System.out.println("c2 has a next so t2 is "+t2);
        	if (p.filter(t1, t2)) {
        		//System.out.println("merges tup desc = "+ getTupleDesc());
        		t = Tuple.merge(t1, t2, getTupleDesc());
        		//System.out.println("they match! and I am about to return "+t);
        		flag = true;
        	}
        	if (!child2.hasNext()) {
        		//System.out.println("I should be at the end of c2");
        		if (child1.hasNext()) {
        			t1 = child1.next();
        			//System.out.println("c1 has a next se t1 is "+t1);
        			child2.rewind();
        		}
        	}
        	if (flag) return t;
        }
    	//System.out.println("should be at the end of both lists, returning null");
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
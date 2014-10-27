package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private int tableid;
    private int iocost;
    private HeapFile file;
    private int[] mins;
    private int[] maxs;
    private Object[] histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	this.tableid = tableid;
    	this.iocost = ioCostPerPage;
    	
    	TransactionId tid = new TransactionId();
    	this.file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	DbFileIterator iter = file.iterator(tid);
    	TupleDesc desc = file.getTupleDesc();
    	int num_fields = desc.numFields();
    	boolean initialize = false;
    	
    	//initialize arrays
    	mins = new int[num_fields];
    	maxs = new int[num_fields];
    	histograms = new Object[num_fields];
    	
    	try {
			iter.open();
			// go through file once to find the min and the max
			while (iter.hasNext()) {
				Tuple tup = iter.next();
				for (int i=0;i<num_fields;i++) {
					Field f = tup.getField(i);
					switch (f.getType()) {
						case INT_TYPE: IntField intfield = (IntField) f;
									   int v = intfield.getValue();
									   if (initialize == false) {
										   mins[i] = v;
										   maxs[i] = v;
									   }
									   else {
										   if (v < mins[i]) { mins[i] = v; }
										   if (v > maxs[i]) { maxs[i] = v; }
									   }
									   break;
						case STRING_TYPE: //no min or max for Strings?
						default: break;
					}
				}
				if (initialize==false) {initialize = true;} //after looked at the first tuple
			}
			//Now, we can initialize the histograms			
			iter.rewind();
			//while (iter.hasNext()) {
				Tuple t = iter.next();
				for (int ii=0;ii<num_fields;ii++) {
					Field fi = t.getField(ii);
					switch (fi.getType()) {
						case INT_TYPE: int low = mins[ii];
									   int high = maxs[ii];
									   histograms[ii] = new IntHistogram(NUM_HIST_BINS, low, high);
									   break;
						case STRING_TYPE: histograms[ii] = new StringHistogram(NUM_HIST_BINS);
										break;
						default: break;
					}
				}
			//}
			
			//Finally, we can add values to the histograms
			iter.rewind();
			while (iter.hasNext()) {
				Tuple tup = iter.next();
				for (int i=0;i<num_fields;i++) {
					Field f = tup.getField(i);
					switch (f.getType()) {
						case INT_TYPE: IntField intfield = (IntField) f;
									   int v = intfield.getValue();
									   IntHistogram hist = (IntHistogram) histograms[i];
									   hist.addValue(v);
									   break;
						case STRING_TYPE: StringField stringfield = (StringField) f;
						   				  String s = stringfield.getValue();
						   				  StringHistogram h = (StringHistogram) histograms[i];
						   				  h.addValue(s);
						default: break;
					}
				}
			}
			 //CLOSE AND COMPLETE TRANSACTION?
			
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.file.numPages()*this.iocost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        TupleDesc desc = file.getTupleDesc();
        int field = 0;
        int count = 0;
        Type type = desc.getFieldType(field);
        
        switch(type) {
        	case INT_TYPE: IntHistogram hist = (IntHistogram) histograms[field];
        				   count = hist.totalVals();
        				   break;
        	case STRING_TYPE: StringHistogram h = (StringHistogram) histograms[field];
        					  count = h.totalVals();
        					  break;
        	default: break;
        }
        System.out.println("count = "+ count);
        return (int)(selectivityFactor*count);
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
        Object histogram = histograms[field];
        if (histogram instanceof IntHistogram) {
        	return ((IntHistogram)histogram).getDistinctVals();
        }
        if (histogram instanceof StringHistogram) {
        	return ((StringHistogram)histogram).getDistinctVals();
        }
        throw new UnsupportedOperationException("implement me");

    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        switch(constant.getType()) {
    	case INT_TYPE: IntField intfield = (IntField) constant;
    				   IntHistogram hist = (IntHistogram) histograms[field];
    				   int v = intfield.getValue();
    				   return hist.estimateSelectivity(op, v);
    	case STRING_TYPE: StringField stringfield = (StringField) constant;
    					  StringHistogram h = (StringHistogram) histograms[field];
    					  String s = stringfield.getValue();
    					  return h.estimateSelectivity(op, s);
    	default: break;
    	}
        return 0.0;
    }

}

package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int numB;
	private int min;
	private int max;
	
	private double range;
	private int total;
	
	private int[] hist;
    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.numB = buckets;
        this.min = min;
        this.max = max;
        
        this.range = ((double)(max-min))/numB;
        this.total = 0;
        
        hist = new int[numB];
        for (int i=0;i<numB;i++) { //empty at first
        	hist[i] = 0;
        }
    }
    
    public double getLowerRange(int index) {
    	return min + (index*range);
    }
    
    public double getUpperRange(int index) {
    	if ( index == numB-1 ) { //last bucket
    		return max;
    	}
    	return getLowerRange(index+1);
    }
    
    public double getRangePop(int index) {
    	return getLowerRange(index) - getUpperRange(index);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	System.out.println("adding value: " + v);
    	System.out.println("min = "+ min);
    	System.out.println("max - "+ max);
    	System.out.println("numB = " + numB);
    	System.out.println("bucket range = " + range);
        if (v<min) {
        	throw new RuntimeException();
        }
        if (v>max) {
        	throw new RuntimeException();
        }
        System.out.println("FINDING");
        int i = find(v);
        System.out.println("FOUND! "+ i);
        hist[i]++;
        total++;
    }
    
    public int find(int v) {
    	int i;
        for (i=0;i<numB;i++) {
        	System.out.println("i = "+i);
        	System.out.println("VALUE "+v);
        	System.out.println("lower = "+getLowerRange(i));
        	System.out.println("upper = "+getUpperRange(i));
        	if (getLowerRange(i)<=v && getUpperRange(i)>v || i==numB-1) {
        		return i;
        	}
        }
        return -1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int index;
    	int num = 0;
    	switch (op) {
    		case EQUALS: index = find(v);
    					 num = hist[index];
    					 return (double)num/total;
    		case GREATER_THAN: if (v<min) return 1.0;
			   				   if (v>max) return 0.0;index = find(v);	
    						   if (index==numB-1) return 0.0;
    						   for (int i=index;i<numB;i++) {
    							   num += hist[i];
    						   }
    						   System.out.println("gt num "+num);
    						   System.out.println("gt total "+total);
    						   System.out.println("gt index "+index);
    						   return (double)num/total;
    	
    	}
        System.out.println("hi at end of eS (sis not return)");
        return -1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}

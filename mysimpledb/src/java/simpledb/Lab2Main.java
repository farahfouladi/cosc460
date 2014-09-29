
package simpledb;

import java.io.*;

public class Lab2Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {		
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("2page_data.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
            	System.out.println("Update tup: " + tup);
                if(tup.getField(1).equals(new IntField(3))) {    
                	
                	//delete tup w/middle val of 1
                	table1.deleteTuple(tid, tup);
                	
                	//now we make new tup to insert
                	Type newTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
                    String newNames[] = new String[]{ "field0", "field1", "field2" };
                    TupleDesc newDescriptor = new TupleDesc(newTypes, newNames);
                    Tuple newTup = new Tuple(newDescriptor);
                    newTup.setField(0, tup.getField(0));
                    newTup.setField(1, new IntField(1));
                    newTup.setField(2, tup.getField(2));
                    
                    //and insert it!
                	table1.insertTuple(tid, newTup);
                	
                	//print it out:
                    System.out.println(" to be: " + newTup);
                    System.out.println();
                }
            }
            f.rewind();
            System.out.println();
            System.out.println("REWIND!");
            System.out.println();
            
            while (f.hasNext()) {
                Tuple tup = f.next();
                if(tup.getField(1).equals(new IntField(10))) { 
                	System.out.println("Deleting Tuple: " + tup);
                	table1.deleteTuple(tid, tup);
                }
            }
            
            //now we make new tup to insert
        	Type newerTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
            String newerNames[] = new String[]{ "field0", "field1", "field2" };
            TupleDesc newDescriptor = new TupleDesc(newerTypes, newerNames);
            Tuple newerTup = new Tuple(newDescriptor);
            newerTup.setField(0, new IntField(99));
            newerTup.setField(1, new IntField(99));
            newerTup.setField(2, new IntField(99));
            
            //and insert it!
            System.out.println("Inserting tuple: 99 99 99");
        	table1.insertTuple(tid, newerTup);
            
            f.rewind();
            while (f.hasNext()) {
            	Tuple datup = f.next();
            	System.out.println("Tuple: " + datup);
            }
        
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        }
        catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}

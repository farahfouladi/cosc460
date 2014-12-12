package simpledb;

import java.io.EOFException;
import java.io.IOException;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {	
    	print();
    	long offset = readOnlyLog.length() - LogFile.LONG_SIZE;
        readOnlyLog.seek(offset); 
        while (offset > 0){
        	System.out.println("Roll back offset = " + offset);
        	long start = readOnlyLog.readLong();
        	readOnlyLog.seek(start);
        	int type = readOnlyLog.readInt();
        	System.out.println("Rollback type = " + type);
        	long tid = readOnlyLog.readLong();
        	System.out.println("Rollback tid = " + tid);
        	
        	if (type == LogType.BEGIN_RECORD) {
        		System.out.println("Log Begin");
        		break;
        	}
        	
        	if (type == LogType.COMMIT_RECORD && tid == tidToRollback.getId()){
        		throw new IOException("Transaction committed");
        	}
        	
        	if (type == LogType.ABORT_RECORD) {
        		throw new IOException("Transaction aborted.");
        	}
        	
        	if (type == LogType.UPDATE_RECORD && tid == tidToRollback.getId()){
        		System.out.println("Log UPDATE");
        		Page before = LogFile.readPageData(readOnlyLog);
        		System.out.println("page before (write) = " + before.getId());
        		Page after = LogFile.readPageData(readOnlyLog);
        		System.out.println("page after = " + after.getId());
    			HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(before.getId().getTableId());
    			file.writePage(before);
    			System.out.println("discarding page = " + before.getId());
    			Database.getBufferPool().discardPage(before.getId());
    			Database.getLogFile().logCLR(tid, after);
        	}
        	
        	if (type == LogType.CLR_RECORD) {
        		break;
        	}
        	
        	if (type == LogType.CHECKPOINT_RECORD) {
        		//ummm...
        		break;
        	}

        	offset = start - LogFile.LONG_SIZE;
        	readOnlyLog.seek(offset);
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
    	readOnlyLog.seek(0);
    	ArrayList<Long> losers = new ArrayList<Long>();
    	Long pos = readOnlyLog.readLong();
    	
    	if (pos != -1) {
    		readOnlyLog.seek(pos);
    		readOnlyLog.readInt();
    		readOnlyLog.readLong();
    		int numTxns = readOnlyLog.readInt();
    		for (int i = 0; i < numTxns; i++) {
    			losers.add(readOnlyLog.readLong());
    		}
    		readOnlyLog.readLong();    		
    	}
    	else {
    		//seek to where? beginning bc no chkpts?
    		readOnlyLog.seek(0); 
    		readOnlyLog.readInt();
    		readOnlyLog.readLong();
    	}
    	
    	while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
    		int type = readOnlyLog.readInt();
    		long tid = readOnlyLog.readLong();
    		
    		//type cases!
    		if (type == LogType.BEGIN_RECORD) {
    			losers.add(tid);
    		}
			if (type == LogType.COMMIT_RECORD) {
			    losers.remove(tid);
			}
			if (type == LogType.ABORT_RECORD) {
				losers.remove(tid);
			}
			if (type == LogType.UPDATE_RECORD) {
				Page before = LogFile.readPageData(readOnlyLog);
				Page after = LogFile.readPageData(readOnlyLog);
				HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(before.getId().getTableId());
				file.writePage(after);
			}
			if (type == LogType.CLR_RECORD) {
				Page before = LogFile.readPageData(readOnlyLog);
				HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(before.getId().getTableId());
				file.writePage(before);
			}
			readOnlyLog.readLong();
    	} //end while
    	
    	//undoing part 
    	readOnlyLog.seek(readOnlyLog.length());
    	pos = readOnlyLog.length();
        while (losers.size() > 0){
        	pos = pos - LogFile.LONG_SIZE;
        	readOnlyLog.seek(pos);
        	pos = readOnlyLog.readLong();
        	readOnlyLog.seek(pos);
        	int type = readOnlyLog.readInt();
        	long tid = readOnlyLog.readLong();
        	
        	if (type == LogType.BEGIN_RECORD){
        		if (losers.contains(tid)){
        			losers.remove(tid);
        			Database.getLogFile().logAbort(tid);
        		}
        	}
        	if (type == LogType.UPDATE_RECORD){
        		if (losers.contains(tid)) {
        			Page before = Database.getLogFile().readPageData(readOnlyLog);
    				HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(before.getId().getTableId());
    				file.writePage(before);
    				Database.getLogFile().logCLR(tid, before);
        		}
        	}

        }
    }
}

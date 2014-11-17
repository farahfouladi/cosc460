package simpledb;
import java.util.*;

/**
 * Lock Manager manages a collection of pages (objects)
 * We can refer to the lock's object by it's pageId
 *
 */

public class LockManager {
	// lock table
	private HashMap<Integer, Lock> lockTable;
	// txn tables
	private HashMap<Integer, ArrayList<Integer>> lockedPages;
	private HashMap<Integer, ArrayList<Integer>> waitingForPages;
	
	
	
	public LockManager() {
		lockTable = new HashMap<Integer, Lock>();
		lockedPages = new HashMap<Integer, ArrayList<Integer>>();
		waitingForPages = new HashMap<Integer, ArrayList<Integer>>();
	}

	/**
	 * Finds out if a lock is available
	 * @param pid
	 * @return true if lock is available, false if unavailable
	 */
	public boolean requestLock(PageId pid, TransactionId tid, Permissions perm) {
        boolean waiting = true;
		while (waiting) {
			synchronized(this) {
				System.out.println("Txn: "+tid.hashCode()+ " is requesting a lock on page "+ pid.hashCode());
                // check if lock is available
    			//if the lock can be shared, this page may acquire the lock
    			Lock lock = getLockInfo(pid,perm);
    			if (lock==null) {}
    			else if (lock.getType().equals("SHARED")) {
    				System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
    				System.out.println("the lock for this page is a shared lock!\n");
    				if (!lockedPages.containsKey(tid.hashCode())) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid.hashCode());
    				}
    				lock.addTransaction(tid.hashCode()); 
    				lockedPages.get(tid.hashCode()).add(pid.hashCode());
    				return true;
    			}
    			
    			else if (lock.getType().equals("UPGRADE")) {
    				if (lock.getTransactions().size() == 1 && lock.getTransactions().get(0)==tid.hashCode()){
        				System.out.println("Can Upgrade to an Exclusive lock");
        				lock.setType("EXCLUSIVE");
        				return true;
    				}
    				else {
    					lock.setType("SHARED");
    				}
    			}
    			
    			else if (lock.getType().equals("DOWNGRADE")) {
    				if (lock.getTransactions().get(0)==tid.hashCode()){
        				System.out.println("Can Downgrade to a Shared lock");
        				lock.setType("SHARED");
        				return true;
    				}
    				else { //another txn has lock
    					lock.setType("EXCLUSIVE");
    				}
    			}
    			
    			//if the lock is exclusive but no one else is holding the lock, this page may acquire the lock
    			else if (lock.getType().equals("EXCLUSIVE") && lock.getTransactions().isEmpty() ) {
    				System.out.println("Number of locks on this page is " + lock.getTransactions().size());
    				System.out.println("the lock for this page is exclusive and availabe");
    				if (!lockedPages.containsKey(tid.hashCode())) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid.hashCode());
    				}
    				lock.addTransaction(tid.hashCode());
    				lockedPages.get(tid.hashCode()).add(pid.hashCode());
    				return true;
    			}
    			// final case: exclusive lock, but another transaction has the lock for this page
    			else {
    				System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
    				System.out.println("the lock for this page is exclusive and taken!");
    				if (!lockedPages.containsKey(tid.hashCode())) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid.hashCode());
    				}
    				lock.addRequests(pid.hashCode());
    				waitingForPages.get(tid.hashCode()).add(pid.hashCode());
    			}
                try {
                   Thread.sleep(1);
                } catch (InterruptedException ignored) { }
    		}	
		}
		return false;
   }
		


	
	public boolean holdsLock(int pid, int tid){
        if (getLockedPages().containsKey(tid)) {
        	ArrayList<Integer> locksList = getLockedPages().get(hashCode());
        	if (locksList.indexOf(pid) >= 0) { 
        		return true;
        	}
        }
    	return false;
	}
	
	public void makeNewTxn(int tid) {
		ArrayList<Integer> arr1 = new ArrayList<Integer>();
		ArrayList<Integer> arr2 = new ArrayList<Integer>();
		lockedPages.put(tid,arr1);
		waitingForPages.put(tid, arr2);
	}
	
	public Lock getLockInfo(PageId pid, Permissions perm) {
		Lock lock = null;
		String type = ""; 
		if (perm!=null) {
			if (perm.toString() == "READ_ONLY") {type = "SHARED";}
			if (perm.toString() == "READ_WRITE") {type = "EXCLUSIVE"; }
		}
		int pageId = pid.hashCode();
		if (lockTable.containsKey(pageId)) {
			lock = lockTable.get(pageId);
			// check if upgrading!
			if (lock.getType().equals("SHARED") && type.equals("EXCLUSIVE")){
				System.out.println("or here?");
					lock.setType("UPGRADE");
			}
			if (lock.getType().equals("EXCLUSIVE") && type.equals("SHARED")){
				System.out.println("here?");
				lock.setType("DOWNGRADE");
			}
		}
		else { // have not used a lock on this object yet
			lock = new Lock(pid.hashCode(),type);
			lockTable.put(pageId,lock);
		}
		return lock;
	}
	
	/**
	 * Given an id, that object must release all of its locks
	 * @param pid
	 * @return true if object successfully releases locks, if not false
	 */
	public boolean releaseLock(int pid){
		return false;
	}
	
	public HashMap<Integer, Lock> getLockTable() {
		return lockTable;
	}

	public HashMap<Integer, ArrayList<Integer>> getLockedPages() {
		return lockedPages;
	}

	public HashMap<Integer, ArrayList<Integer>> getWaitingForPages() {
		return waitingForPages;
	}

}

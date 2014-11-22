package simpledb;
import java.util.*;

/**
 * Lock Manager manages a collection of pages (objects)
 * We can refer to the lock's object by it's pageId
 *
 */

public class LockManager {
	// lock table
	private HashMap<PageId, Lock> lockTable;  //pid to lock
	// txn tables
	private HashMap<TransactionId, ArrayList<PageId>> lockedPages;  //tid to list of pids
	private HashMap<TransactionId, ArrayList<PageId>> waitingForPages;  //tid to list of pids
	
	
	
	public LockManager() {
		lockTable = new HashMap<PageId, Lock>();
		lockedPages = new HashMap<TransactionId, ArrayList<PageId>>();
		waitingForPages = new HashMap<TransactionId, ArrayList<PageId>>();
	}

	/**
	 * Finds out if a lock is available
	 * @param pid
	 * @return true if lock is available, false if unavailable
	 * @throws TransactionAbortedException 
	 */
	public boolean requestLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        long startTime = System.currentTimeMillis();
		while (waiting) {
			if (System.currentTimeMillis() - startTime > 100) {
				throw new TransactionAbortedException();
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			synchronized(this) {

                // check if lock is available
    			//if the lock can be shared, this page may acquire the lock
    			Lock lock = getLockInfo(pid,perm);
				System.out.println("Txn: "+tid+ " is requesting a " + lock.getType() +" lock on page "+ pid);
    			if (lock==null) {}
    			else if (lock.getType().equals("SHARED")) {
    				//System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
    				System.out.println("the lock for this page is a shared lock!\n");
    				if (!lockedPages.containsKey(tid)) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid);
    				}
    				lock.addTransaction(tid);
    				waitedForLock(tid,pid);
    				lockedPages.get(tid).add(pid);
    				//System.out.println("SIZEEE????? " + lock.getTransactions().size());
    				return true;
    			}
    			
    			else if (lock.getType().equals("UPGRADE")) {
    				if (lock.getTransactions().size() == 1 && lock.getTransactions().get(0)==tid){
        				System.out.println("Can Upgrade to an Exclusive lock");
        				lock.setType("EXCLUSIVE");
        				return true;
    				}
    				else {
    					lock.setType("SHARED");
    				}
    			}
    			
    			else if (lock.getType().equals("DOWNGRADE")) {
    				if (lock.getTransactions().get(0)==tid){
        				System.out.println("Can Downgrade to a Shared lock");
        				lock.setType("SHARED");
        				return true;
    				}
    				else { //another txn has lock
    					lock.setType("EXCLUSIVE");
    				}
    			}
    			
    			//if the lock is exclusive but no one else is holding the lock, this page may acquire the lock
    			else if (lock.getType().equals("EXCLUSIVE") && isXAvailable(lock,tid) ) {
    				//System.out.println("Number of locks on this page is " + lock.getTransactions().size());
    				System.out.println("the lock for this page is exclusive and availabe");
    				if (!lockedPages.containsKey(tid)) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid);
    				}
    				lock.addTransaction(tid);
    				waitedForLock(tid, pid);
    				lockedPages.get(tid).add(pid);
    				return true;
    			}
    			// final case: exclusive lock, but another transaction has the lock for this page
    			else {
    				System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
    				System.out.println("the lock for this page is exclusive and taken!");
    				if (!lockedPages.containsKey(tid)) {
    					//update tables to accommodate the new txn
    					makeNewTxn(tid);
    				}
    				lock.addRequests(pid);
    				waitingForPages.get(tid).add(pid);
    			}
                try {
                   Thread.sleep(1);
                } catch (InterruptedException ignored) { }
    		}	
		}
		return false;
   }
		
	//checks if exclusive lock is availabe
	private boolean isXAvailable(Lock lock, TransactionId tid) {
		System.out.println("checking X availability size = " + lock.getTransactions().size());
		if (lock.getTransactions().isEmpty()) {
			return true;
		}
		if (lock.getTransactions().size()==1 && lock.getTransactions().get(0)==tid) {
			return true;
		}
		return false;
	}

	private void waitedForLock(TransactionId tid, PageId pid) {
		for (PageId pageid : waitingForPages.get(tid)) {
			if (pageid == pid) {
				waitingForPages.remove(pid);
			}
		}
	}

	
	public boolean holdsLock(PageId pid, TransactionId tid){
        if (getLockedPages().containsKey(tid)) {
        	ArrayList<PageId> locksList = getLockedPages().get(tid);
        	if (locksList.indexOf(pid) >= 0) { 
        		return true;
        	}
        }
    	return false;
	}
	
	public void makeNewTxn(TransactionId tid) {
		ArrayList<PageId> arr1 = new ArrayList<PageId>();
		ArrayList<PageId> arr2 = new ArrayList<PageId>();
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
		PageId pageId = pid;
		if (lockTable.containsKey(pageId)) {
			lock = lockTable.get(pageId);
			// check if upgrading!
			if (lock.getType().equals("SHARED") && type.equals("EXCLUSIVE")){
					lock.setType("UPGRADE");
			}
			if (lock.getType().equals("EXCLUSIVE") && type.equals("SHARED")){
				System.out.println("here?");
				lock.setType("DOWNGRADE");
			}
		}
		else { // have not used a lock on this object yet
			lock = new Lock(pid,type);
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
	
	public HashMap<PageId, Lock> getLockTable() {
		return lockTable;
	}

	public HashMap<TransactionId, ArrayList<PageId>> getLockedPages() {
		return lockedPages;
	}

	public HashMap<TransactionId, ArrayList<PageId>> getWaitingForPages() {
		return waitingForPages;
	}

}
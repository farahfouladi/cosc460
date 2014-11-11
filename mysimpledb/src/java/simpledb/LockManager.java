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
		//synchronized(pid) {	// 2 threads with the same pageId should not be able to execute this code concurrently
			System.out.println("Txn: "+tid.hashCode()+ " is requesting a lock on page "+ pid.hashCode());
			Lock lock = getLockInfo(pid,perm);
			if (!lockedPages.containsKey(tid.hashCode())) {
				//update tables to accommodate the new txn
				makeNewTxn(tid.hashCode());
			}
			//if the lock can be shared, this page may acquire the lock
			if (lock.getType().equals("SHARED")) {
				System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
				System.out.println("the lock for this page is a shared lock!\n");
				lock.addTransaction(tid.hashCode()); 
				lockedPages.get(tid.hashCode()).add(pid.hashCode());
				return true;
			}
			
			//check if upgrading
			
			//if the lock is exclusive but no one else is holding the lock, this page may acquire the lock
			else if (lock.getType().equals("EXCLUSIVE") && lock.getTransactions().isEmpty() ) {
				System.out.println("Number of locks on this page is " + lock.getTransactions().size());
				System.out.println("the lock for this page is exclusive and availabe");
				lock.addTransaction(tid.hashCode());
				lockedPages.get(tid.hashCode()).add(pid.hashCode());
				return true;
			}
			// final case: exclusive lock, but another transaction has the lock for this page
			else {
				System.out.println("Number of locks on this page is " + lock.getTransactions().size());				
				System.out.println("the lock for this page is exclusive and taken!");
				lock.addRequests(pid.hashCode());
				waitingForPages.get(tid.hashCode()).add(pid.hashCode());
				return false;
			}
		//}
	}
	
	public void makeNewTxn(int tid) {
		System.out.println("NEW TXN!");
		ArrayList<Integer> arr1 = new ArrayList<Integer>();
		ArrayList<Integer> arr2 = new ArrayList<Integer>();
		lockedPages.put(tid,arr1);
		waitingForPages.put(tid, arr2);
	}
	
	public Lock getLockInfo(PageId pid, Permissions perm) {
		Lock lock = null;
		int pageId = pid.hashCode();
		if (lockTable.containsKey(pageId)) {
			lock = lockTable.get(pageId);
			System.out.println("here?");
		}
		else { // have not used a lock on this object yet
			System.out.println("NEW LOCK / PAGE !");
			if (perm.toString() == "READ_ONLY") {
				lock = new Lock(pid.hashCode(),"SHARED");
			}
			else if (perm.toString() == "READ_WRITE") {
				lock = new Lock(pid.hashCode(), "EXCLUSIVE"); 
			}
			else {
				System.out.println("Permissions not set correctly");
			}
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

package simpledb;
import java.util.*;

/**
 * Lock Manager manages a collection of pages (objects)
 * We can refer to the lock's object by it's pageId
 *
 */

public class LockManager {
	
	private HashMap<Integer, Lock> lockTable;
	private HashMap<Integer, ArrayList<Integer>> lockedPages;
	private HashMap<Integer, ArrayList<Integer>> waitingForPages;
	
	/**
	 * Finds out if a lock is available
	 * @param pid
	 * @return true if lock is available, false if unavailable
	 */
	public boolean requestLock(PageId pid, TransactionId tid, Permissions perm) {
		synchronized(pid) {	// 2 threads with the same pageId should not be able to execute this code concurrently
			Lock lock = getLockInfo(pid,perm);
			//if the lock can be shared, this page may acquire the lock
			if (lock.getType().equals("SHARED")) {
				lock.addTransaction(tid.hashCode()); 
				lockedPages.get(tid.hashCode()).add(pid.hashCode());
				return true;
			}
			
			//check if upgrading
			
			//if the lock is exclusive but no one else is holding the lock, this page may acquire the lock
			else if (lock.getType().equals("EXCLUSIVE") && lock.getTransactions().isEmpty() ) {
				lock.addTransaction(tid.hashCode());
				lockedPages.get(tid.hashCode()).add(pid.hashCode());
				return true;
			}
			// final case: exclusive lock, but another transaction has the lock for this page
			else {
				lock.addRequests(pid.hashCode());
				waitingForPages.get(tid.hashCode()).add(pid.hashCode());
				return false;
			}
		}
	}
	
	public Lock getLockInfo(PageId pid, Permissions perm) {
		Lock lock = null;
		int pageId = pid.hashCode();
		if (lockTable.containsKey(pageId)) {
			lock = lockTable.get(pageId);
			}
		else { // have not used a lock on this object yet
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

}

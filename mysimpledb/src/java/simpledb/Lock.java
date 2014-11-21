package simpledb;

import java.util.*;

public class Lock {
	
	private int id;
	private ArrayList<TransactionId> lockedTxns;
	private String type; //should probably makes these constants?
	private Queue<PageId> txnRequests;
	
	public Lock(int id, String type) {
		this.id = id;
		this.type = type;
		lockedTxns = new ArrayList<TransactionId>();
		txnRequests = new LinkedList<PageId>();
	}
	
	public int getId() {
		return id;
	}
	
	public ArrayList<TransactionId> getTransactions() {
		return lockedTxns;
	}
	
	public void addTransaction(TransactionId tid) {
		this.lockedTxns.add(tid);
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}

	public Queue<PageId> getRequests() {
		return txnRequests;
	}
	public void addRequests(PageId pid) {
		this.txnRequests.add(pid);
	}
	
	

}

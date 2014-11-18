package simpledb;

import java.util.*;

public class Lock {
	
	private int id;
	private ArrayList<Integer> lockedTxns;
	private String type; //should probably makes these constants?
	private Queue<Integer> txnRequests;
	
	public Lock(int id, String type) {
		this.id = id;
		this.type = type;
		lockedTxns = new ArrayList<Integer>();
		txnRequests = new LinkedList<Integer>();
	}
	
	public int getId() {
		return id;
	}
	
	public ArrayList<Integer> getTransactions() {
		return lockedTxns;
	}
	
	public void addTransaction(int tid) {
		this.lockedTxns.add(tid);
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}

	public Queue<Integer> getRequests() {
		return txnRequests;
	}
	public void addRequests(int pid) {
		this.txnRequests.add(pid);
	}
	
	

}

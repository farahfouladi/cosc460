package concurrency_tutorial;

public class ZigZagThreads {
    private static final LockManager lm = new LockManager();
    public static LockManager getLockManager() { return lm; }

    public static void main(String args[]) throws InterruptedException {
        int numZigZags = 10;
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zigger()).start();
        }
        for (int i = 0; i < numZigZags; i++) {
            new Thread(new Zagger()).start();
        }
    }

    static class Zigger implements Runnable {

        protected String myPattern;
        protected boolean isZigger;

        public Zigger() {
            myPattern = "//////////";
            isZigger = true;
        }

        public void run() {
            ZigZagThreads.getLockManager().acquireLock(isZigger);
            System.out.println(myPattern);
            ZigZagThreads.getLockManager().releaseLock();
        }
    }

    static class Zagger extends Zigger {

        public Zagger() {
            myPattern = "\\\\\\\\\\\\\\\\\\\\";
            isZigger = false;
        }

    }

    static class LockManager {
        private boolean inUse = false;
        private boolean needZig = true;

        private synchronized boolean isLockFree(boolean isZigger) {
            if (!inUse && (isZigger == needZig)) {
            	return true;
            }
            return false;
        }

        public synchronized void acquireLock(boolean isZigger) {
        	while (!isLockFree(isZigger)) {
                // check if lock is available
            	try {
            		wait();
            	}
            	catch (InterruptedException ignored) { }
            }
            inUse = true;
            
            if (needZig) {
            	needZig = false;
            }
            else {
            	needZig = true;
            }
            
            notifyAll();        
        }

        public synchronized void releaseLock() {
            inUse = false;
            notifyAll();
       }
    }}


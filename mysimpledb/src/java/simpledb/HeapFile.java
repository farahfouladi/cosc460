package simpledb;

import java.io.*;
import java.util.*;


/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	private File f;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode()/100;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	//System.out.println("HERE?");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	System.out.println("READING FROM DISK");
    	int pgNo = pid.pageNumber();
        byte[] b = new byte[BufferPool.getPageSize()];
        BufferedInputStream fis;
        Page pg;
        try {
        	//System.out.println("file (in heap file read method) = "+ f);
			fis = new BufferedInputStream(new FileInputStream(f));
			fis.skip( (pgNo)*BufferPool.getPageSize() );
			int bytesRead = fis.read(b,0,BufferPool.getPageSize());
			if(bytesRead != BufferPool.getPageSize()){
				throw new RuntimeException();
			}
	    fis.close();
	    HeapPageId hpid = ((HeapPageId) pid);
	    //System.out.println("pid in read page method is " + hpid.hashCode());
	    System.out.println("read the page from file");
	    pg = new HeapPage(hpid,b); 
		} catch (Exception e) {
			System.out.println("error in read page");
			throw new IllegalArgumentException();
		}
       return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	System.out.println("!!!!!!!!!! ABOUT TO WRITE PAGE " + page);
    	 try {
    		 PageId pid= page.getId();
    		 HeapPageId hpid= (HeapPageId)pid;
    		 RandomAccessFile file = new RandomAccessFile(f,"rw");
    		 int offset = pid.pageNumber()*BufferPool.getPageSize();
    		 byte[] b=new byte[BufferPool.getPageSize()];
    		 b=page.getPageData();
    		 System.out.println(Arrays.toString(b));
    		 file.seek(offset);
    		 file.write(b, 0, BufferPool.getPageSize());
    		 file.close();
    	} catch (Exception e) {
        	System.out.println("error in write page");
        	throw new IllegalArgumentException();
        }
        
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int fileSize = (int)f.length();
        int pageSize = BufferPool.PAGE_SIZE;
        //System.out.println("File size: " + fileSize);
        //System.out.println("Number of Pages: " + ((int)Math.ceil(fileSize/(pageSize))));
        //System.out.println(fileSize);
        return (int)Math.ceil(fileSize/(pageSize));
    }

    
    public PageId findPage(TransactionId tid) throws DbException, IOException, TransactionAbortedException {
    	System.out.println("findpage");
    	boolean haveLockFlag = false;
    	HeapPage page = null;
    	
    	for (int i = 0; i < numPages(); i++) {
    		
    		HeapPageId hpid = new HeapPageId(getId(), i);
    		haveLockFlag = Database.getBufferPool().holdsLock(tid, hpid);
    		page = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
    		if (page.getNumEmptySlots() != 0) {
    			return hpid;
    		}
    		else {
    			if (!haveLockFlag) {
    				System.out.println("this one???");
    				Database.getBufferPool().releasePage(tid, hpid);
    			}
    		}
    	}
    	//exits for loop must mean that all pages in bpool are full -- create new page
    	HeapPageId newHpid = new HeapPageId(getId(), numPages());
		HeapPage newPg = new HeapPage(newHpid, HeapPage.createEmptyPageData());
		writePage(newPg);
		return newHpid;
    }
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> list = new ArrayList<Page>();
	    synchronized(this) {
    		HeapPageId hpid = (HeapPageId)findPage(tid);
    		HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
			pg.insertTuple(t);
			pg.markDirty(true, tid);
			list.add(pg);
	    }
		return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> list = new ArrayList<Page>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage pg;
        synchronized(this) {
        	pg = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        	pg.deleteTuple(t);
        	pg.markDirty(true, tid);
        	list.add(pg);
        }
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	DbFileIterator dbItr = new HeapFileIterator(tid);
        return dbItr;
    }
    
    class HeapFileIterator implements DbFileIterator {
    	
    	private int pgNo;
    	private Iterator<Tuple> tupItr = null;
    	private Tuple next = null;
    	private TransactionId tid;
    	private Iterable<Tuple> tuples = null;
    	
    	public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			 pgNo = 0;
             tuples = getTupsNextPage(pgNo);
             tupItr = tuples.iterator();
			
			/*System.out.println("I am in the heapfile terator opens method");
	    	BufferPool bp;
	    	HeapPageId hpId;
			bp = Database.getBufferPool();
			
			hpId = new HeapPageId(getId(), pgNo);
			
			Page page;
			Permissions perm = null; // what are the permissions = problem??
			try {
				page = bp.getPage(tid, hpId, perm);
				tupItr = new TupleIterator(td, (HeapPage)page);
				System.out.println(tupItr);
				tupItr.open();
			}
			catch(Exception e) {
			
			}*/
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
	       	 //System.out.println("pgNo we are on " + pgNo);
			 if( tupItr == null){
	             return false;
			 }
			 
	         if(tupItr.hasNext()){
	                 // next tuple found in this page
	                 return true;
	         } 
	         else if (!tupItr.hasNext() && pgNo < numPages()-1){
	             //HeapPageId hpid = new HeapPageId(getId(),pgNo);
	             //Database.getBufferPool().releasePage(tid, hpid);
                 //List<Tuple> nextPgTups = getTupsNextPage(pgNo + 1);
                 //System.out.println("size of next page tups :" + nextPgTups);
                 //if(nextPgTups.size() != 0){
                	 //System.out.println("returning true?");
                	 return true;
                 //} 
                 //else {
                 	//return false;
                 //}
	         } 
	         else {
	                 // no tuple on this page and no more pages
	                 return false;
	         }

//			if (tupItr == null){
//				System.out.println("***1  " + pgNo);
//				return false;	
//			}
//			if (next != null) {
//				System.out.println("***2  " + pgNo);
//				return true;
//			}
//			else {
//				System.out.println("***3  " + pgNo);
//				fetchNext();
//				return next != null;
//			}
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			            if(tupItr == null){
            	throw new NoSuchElementException("open() not called on iterator");
            }
           
            if(tupItr.hasNext()){
            	//System.out.println("hasnext tup");
                Tuple t = tupItr.next();
                return t;
            } 
            else if(!tupItr.hasNext() && pgNo < numPages()-1) {   //***********doesnt go in here ever
               //go to next pg
            	//System.out.println("###### getting next page");
            	HeapPageId hpid = new HeapPageId(getId(),pgNo);
            	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!1");
                Database.getBufferPool().releasePage(tid, hpid);
            	pgNo += 1;
                tuples = getTupsNextPage(pgNo);
                tupItr = tuples.iterator();
                if (tupItr.hasNext())
                	return tupItr.next();
                else {
                	throw new NoSuchElementException("No more Tuples");
                }
            } 
            else {
                throw new NoSuchElementException("No more Tuples");
            }

			
			
//			if(tupItr == null){
//				throw new NoSuchElementException("call open()");
//			}
//			
//			if (!hasNext()) {
//	            throw new NoSuchElementException("nope");
//	        }
//			if (next == null) {
//				throw new NoSuchElementException();
//			}
//			Tuple toReturn = next;
//			next = null;
//			return toReturn;
			
		}
		
		/*private void fetchNext() throws TransactionAbortedException, DbException {
			
			if (tupItr.hasNext()) {
				next = tupItr.next();
			}
			else if (tupItr.hasNext() && pgNo < numPages()-1) {  //not at last page
				pgNo += 1;
				List<Tuple> tupsNextPage = getTupsNextPage(pgNo);
                tupItr = tupsNextPage.iterator();
                if(tupsNextPage.size() != 0) {
                	tupsNextPage.next()
                }
				*/
				
				/*this.pgNo += 1;
				System.out.println("THE PAGE NO IS: " + pgNo);
				hpId = new HeapPageId(getId(), pgNo);
				HeapPage page = (HeapPage) bp.getPage(tid, hpId, perm);
				tupItr.close();
				next = null;
				tupItr = new TupleIterator(td, page);
				tupItr.open();
				if (tupItr.hasNext()) {
					next = tupItr.next();
				}*/
		
		
		
		private List<Tuple> getTupsNextPage(int pgNo) throws TransactionAbortedException, DbException {
			HeapPageId hpId = new HeapPageId(getId(), pgNo);
			Page page = Database.getBufferPool().getPage(tid, hpId, Permissions.READ_ONLY);
			List<Tuple> tups = new ArrayList<Tuple>();
			HeapPage hpage = (HeapPage)page;
			//System.out.println("num of empty slots: " + hpage.getNumEmptySlots());
			Iterator<Tuple> Itr = hpage.iterator();
			while(Itr.hasNext()){ 		//this says there is no tuples on new page...but there are 3 pages and we only go on pg0
				tups.add(Itr.next());
			}
			//System.out.println(tups);
			return tups;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		@Override
		public void close() {
			//next = null;
			//tupItr.close();
			tupItr = null;
		}
    	
    }
}

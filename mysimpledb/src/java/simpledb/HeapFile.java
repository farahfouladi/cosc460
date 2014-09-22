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
    	System.out.println("reading page from OS");
    	int pgNo = pid.pageNumber(); 
        byte[] b = new byte[BufferPool.getPageSize()];
        OutputStream ous = new ByteArrayOutputStream();
        FileInputStream fis;
        int read = 0;
        Page pg;
        try {
        	System.out.println(f);
			fis = new FileInputStream(f);
			fis.skip( (pgNo)*BufferPool.getPageSize() );
			while ( (read = fis.read(b)) != -1){ //reads up to b.length bytes of data from the input stream
				ous.write(b,0,read);
		}
	    ous.close();
	    fis.close();
	    HeapPageId hpid = ((HeapPageId) pid);
	    System.out.println("pid in read page method is " + hpid.hashCode());
	    pg = new HeapPage(hpid,b);  //ERROR IS HERE
		} catch (Exception e) {
			System.out.println("error in read page");
			throw new IllegalArgumentException();
		}
       return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int fileSize = (int)f.length();
        int pageSize = BufferPool.PAGE_SIZE;
        return (int)Math.ceil(pageSize/fileSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	DbFileIterator dbItr = new HeapFileIterator(tid);
        return dbItr;
    }
    
    class HeapFileIterator implements DbFileIterator {
    	
    	private int pgNo;
    	private TupleIterator tupItr;
    	private Tuple next;
    	private TransactionId tid;
    	
    	public HeapFileIterator(TransactionId tid) {
			// assume starting at first page
			pgNo = 0;
			this.tid = tid;
			next = null;
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			System.out.println("I am in the heapfile terator opens method");
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
			
			}
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			
			if (tupItr == null){
				return false;	
			}
			if (next != null) {
				return true;
			}
			else {
				fetchNext();
				return next != null;
			}
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			
			if (!hasNext()) {
	            throw new NoSuchElementException("");
	        }
			if (next == null) {
				throw new NoSuchElementException();
			}
			Tuple toReturn = next;
			next = null;
			return toReturn;
			
		}
		
		private void fetchNext() throws TransactionAbortedException, DbException {
			Permissions perm = null;
			BufferPool bp;
	    	HeapPageId hpId;
			bp = Database.getBufferPool();
			
			if (tupItr.hasNext()) {
				next = tupItr.next();
			}
			else if (pgNo < numPages()) {  //not at last page
				this.pgNo += 1;
				hpId = new HeapPageId(getId(), pgNo);
				HeapPage page = (HeapPage) bp.getPage(tid, hpId, perm);
				tupItr.close();
				next = null;
				tupItr = new TupleIterator(td, page);
				tupItr.open();
				if (tupItr.hasNext()) {
					next = tupItr.next();
				}
			}
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.pgNo = 0;
		}

		@Override
		public void close() {
			next = null;
			//tupItr.close();
			tupItr = null;
		}
    	
    }

}


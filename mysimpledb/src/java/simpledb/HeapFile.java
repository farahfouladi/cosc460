package simpledb;

import java.io.*;
import java.util.*;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

import sun.misc.IOUtils;

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
    	System.out.println("HERE?");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pgNo = pid.pageNumber(); 
        byte[] b = new byte[BufferPool.getPageSize()];
        ByteOutputStream ous = new ByteOutputStream();
        FileInputStream fis;
        int read = 0;
        Page pg;
        try {
			fis = new FileInputStream(f);
			fis.skip( (pgNo)*BufferPool.getPageSize() );
			while ( (read = fis.read(b)) != -1){ //reads up to b.length bytes of data from the input stream
				ous.write(b,0,read);
			}
	    ous.close();
	    fis.close();
	    pg = new HeapPage((HeapPageId)pid,b);
		} catch (Exception e) {
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
    	private TransactionId tid;
    	private TupleIterator tupItr;
    	private HeapPage cur_page;
    	private BufferPool bp;
    	private HeapPageId hpId;
    	
    	public HeapFileIterator(TransactionId tid) {
			// assume starting at first page
			pgNo = 0;
			bp = Database.getBufferPool();
			this.tid = tid;
			hpId = new HeapPageId(getId(), pgNo);
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			System.out.println("opening HeapFileIterator");
			HeapPage page;
			Permissions perm = null; // what are the permissions = problem??
			try {
				page = (HeapPage) bp.getPage(tid, hpId, perm);
				tupItr = new TupleIterator(td, page);
				tupItr.open();
			}
			catch(Exception e) {
			
			}
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (tupItr == null) {
				return false;
			}
			else if (tupItr.hasNext()) {
				return true;
			}
			else if (pgNo < numPages()-1) {
				return true;
			}
			return false;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
		Permissions perm = null;
		if (tupItr == null) {
			throw new NoSuchElementException();
		}
		try {
			if (tupItr.hasNext()) {
				return tupItr.next();
			}
			else if (pgNo < numPages()-1) {
				this.pgNo = pgNo+1; // go to the next page
				HeapPageId hpId = new HeapPageId(getId(), pgNo);
				HeapPage page = (HeapPage) bp.getPage(tid, hpId, perm);
				tupItr.close();
				tupItr = new TupleIterator(td, page);
				tupItr.open();
				if (tupItr.hasNext()) {
					return tupItr.next();
				}
			}
			else {
				throw new NoSuchElementException();
			}
		}
		catch(Exception e) {
			throw new NoSuchElementException();
		}	
		return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.pgNo = 0;
			
		}

		@Override
		public void close() {
			tupItr.close();
		}
    	
    }

}


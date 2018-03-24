import java.io.*;
import java.util.*;


public class HeapFile implements DbFile {

    private int tableId;
    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.tableId = f.getAbsoluteFile().hashCode();
        this.f = f;
        this.td = td;
    }


    public File getFile() {
        return f;
    }

    public int getId() {
        return tableId;
    }

    public TupleDesc getTupleDesc<X, Y> {
        public final X x;
        public final Y y;
        return td;
    }


    public Page readPage(PageId pid) {

        if (this.getId() != pid.getTableId()) return null;
        if (pid.pageNumber() < 0 || pid.pageNumber() >= this.numPages()) return null;
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] byteStream = new byte[pageSize];
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(pageSize * pid.pageNumber());
            raf.readFully(byteStream);
            raf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), byteStream);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
}
    public void writePage(Page page) throws IOException {

        assert page instanceof HeapPage : "Write non-heap page to a heap file.";
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.seek(BufferPool.getPageSize() * page.getId().pageNumber());
        raf.write(page.getPageData());
        raf.close();
    }

    public int numPages() {

        return (int) Math.ceil((double)f.length() / BufferPool.getPageSize());
    }


    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        if (!td.equals(t.getTupleDesc())) throw new DbException("TupleDesc does not match.");
        int i = 0;
        HeapPage hp = null;
        for (i = 0; i < numPages(); i ++) {
            if (((HeapPage)(Database.getBufferPool().getPage(
                                tid, new HeapPageId(tableId, i), Permissions.READ_ONLY))).getNumEmptySlots() > 0)
                break;
        }
        if (i == numPages()) {

            synchronized(this) {
                i = numPages();
                // All files are full
                hp = new HeapPage(new HeapPageId(tableId, i), HeapPage.createEmptyPageData());
                try {
                    int pageSize = BufferPool.getPageSize();
                    byte[] byteStream = hp.getPageData();
                    RandomAccessFile raf = new RandomAccessFile(f, "rw");
                    raf.seek(pageSize * i);
                    raf.write(byteStream);
                    raf.close();
                }
                catch (IOException e) {
                    throw e;
                }
            }
        }
        hp = (HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(tableId, i), Permissions.READ_WRITE));
        hp.insertTuple(t);
        //System.out.println("Tid is" + tid.toString() + " Insert Tuple is" + ((IntField)(t.getField(0))).getValue());
        ArrayList<Page> pList = new ArrayList<Page>();
        pList.add(hp);
        return pList;
    }


    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        if (tableId != t.getRecordId().getPageId().getTableId()) throw new DbException("Table Id does not match.");
        int pageno = t.getRecordId().getPageId().pageNumber();
        if (pageno < 0 || pageno >= numPages()) throw new DbException("Page number is illegal.");
        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE));
        hp.deleteTuple(t);
        //System.out.println("Tid is" + tid.toString() + " Delete Tuple is" + ((IntField)(t.getField(0))).getValue());
        ArrayList<Page> pList = new ArrayList<Page>();
        pList.add(hp);
        return pList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private HeapFile hf;

        private boolean active;
        private int currentPageNo;
        private Iterator<Tuple> currentPageIter;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.tid = tid;
            this.hf = hf;
            close();
        }

        private int numPages() {
            return hf.numPages();
        }

        public void open() throws DbException, TransactionAbortedException {
            active = true;
            currentPageNo = -1;
            currentPageIter = null;
            while (currentPageNo + 1 < numPages()) {
                currentPageNo ++;
                currentPageIter = ((HeapPage)Database.getBufferPool().getPage(
                        tid, new HeapPageId(tableId, currentPageNo), Permissions.READ_ONLY)).iterator();
                if (!hasNext()) continue;
                return;
            }
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            return (currentPageIter != null) && (currentPageIter.hasNext());
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!active) throw new NoSuchElementException("Iterator has not been opened.");
            Tuple ans = (hasNext()) ? currentPageIter.next() : null;
            if (!hasNext()) {
                while (currentPageNo + 1 < numPages()) {
                    currentPageNo ++;
                    currentPageIter = ((HeapPage)Database.getBufferPool().getPage(
                            tid, new HeapPageId(tableId, currentPageNo), Permissions.READ_ONLY)).iterator();
                    if (!hasNext()) continue;
                    break;
                }
            }
            return ans;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            if (!active) throw new DbException("Iterator has not been opened.");
            close();
            open();
        }

        public TupleDesc getTupleDesc() {
            return hf.getTupleDesc();
        }

        public void close() {
            active = false;
            currentPageNo = -1;
            currentPageIter = null;
        }
    }
}
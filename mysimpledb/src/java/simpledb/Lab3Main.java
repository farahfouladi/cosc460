package simpledb;

import java.io.IOException;
import java.util.ArrayList;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
        Filter filterStudents = new Filter(p, scanStudents);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t"+tup);
        }
        filterStudents.close();
        Database.getBufferPool().transactionComplete(tid);
        
        System.out.println();
        System.out.println("Main For Exercise 5 part 1:");
        System.out.println("-------------------------------------");
        TransactionId tId = new TransactionId();
        SeqScan scanCourses = new SeqScan(tId, Database.getCatalog().getTableId("Courses"));
        SeqScan scanProfs = new SeqScan(tId, Database.getCatalog().getTableId("Profs"));
        JoinPredicate jp = new JoinPredicate(0, Predicate.Op.EQUALS, 2);
        Join joinCoursesAndProfs = new Join(jp,scanCourses,scanProfs);
        
        System.out.println("Results:");
        joinCoursesAndProfs.open();
        while (joinCoursesAndProfs.hasNext()) {
            Tuple tup = joinCoursesAndProfs.next();
            System.out.println("\t"+tup);
        }
        joinCoursesAndProfs.close();
        Database.getBufferPool().transactionComplete(tId);
        
        System.out.println();
        System.out.println("Main For Exercise 5 part 2:");
        System.out.println("-------------------------------------");
        TransactionId tID = new TransactionId();
        SeqScan scanStud = new SeqScan(tId, Database.getCatalog().getTableId("Students"));
        SeqScan scanTakes = new SeqScan(tId, Database.getCatalog().getTableId("Takes"));
        JoinPredicate join = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinSandT = new Join(join,scanStud,scanTakes);
        
        System.out.println("Results:");
        joinSandT.open();
        while (joinSandT.hasNext()) {
            Tuple tup = joinSandT.next();
            System.out.println("\t"+tup);
        }
        joinSandT.close();
        Database.getBufferPool().transactionComplete(tID);
        
        System.out.println();
        System.out.println("Main For Exercise 5 part 3 :");
        System.out.println("-------------------------------------");
        TransactionId t = new TransactionId();
        SeqScan scanS = new SeqScan(tId, Database.getCatalog().getTableId("Students"));
        SeqScan scanT = new SeqScan(tId, Database.getCatalog().getTableId("Takes"));
        SeqScan scanP = new SeqScan(tId, Database.getCatalog().getTableId("Profs"));
        JoinPredicate join_ST_equals = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinST = new Join(join_ST_equals,scanS,scanT);
        JoinPredicate join_P_equals = new JoinPredicate(3, Predicate.Op.EQUALS, 2);
        Join joinP = new Join(join_P_equals,joinST,scanP);
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate profP = new Predicate(5, Predicate.Op.EQUALS, hay);
        Filter filter = new Filter(profP, joinP);
        ArrayList<Integer> listids = new ArrayList<Integer>();
        listids.add(1);
        ArrayList<Type> types = new ArrayList<Type>();
        types.add(Type.STRING_TYPE);
        Project proj = new Project(listids,types,filter);
        
        System.out.println("Results:");
        proj.open();
        while (proj.hasNext()) {
            Tuple tup = proj.next();
            System.out.println("\t"+tup);
        }
        proj.close();
        Database.getBufferPool().transactionComplete(t);
    }

}

package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
	private ArrayList<DbFile> tables;
	private ArrayList<String> names;
	private ArrayList<String> keys;
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tables = new ArrayList<DbFile>();
        names = new ArrayList<String>();
        keys = new ArrayList<String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     *                  conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	//adding all elements to the end of the list
        tables.add(file);
        names.add(name);
        keys.add(pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        int ind;
        DbFile file;
        int id;
    	try {
        	ind = names.indexOf(name);
        	file = tables.get(ind);
        	id = file.getId();
        }
        catch(Exception e) {
        	throw new NoSuchElementException();
        }
        return id;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        int i;
        int size = tables.size();
        TupleDesc td = null;
    	DbFile file = null;
        if (size==0) {
        	throw new NoSuchElementException();
        }
        try {
        	for (i=0;i<size;i++) {
        		file = tables.get(i);
        		if (file.getId() == tableid) {
        			break;
        		}
        	}
            td = file.getTupleDesc();
        }
        catch(Exception e) {
        	throw new NoSuchElementException();
        }
        return td;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        int i;
        int size = tables.size();
    	DbFile file = null;
        if (size==0) {
        	throw new NoSuchElementException();
        }
        try {
        	for (i=0;i<size;i++) {
        		file = tables.get(i);
        		if (file.getId() == tableid) {
        			break;
        		}
        	}
        }
        catch(Exception e) {
        	throw new NoSuchElementException();
        }
        return file;
    }

    public String getPrimaryKey(int tableid) {
        int i;
        int size = tables.size();
        int index = -1;
    	DbFile file = null;
    	String pkey;
        if (size==0) {
        	throw new NoSuchElementException();
        }
        try {
        	for (i=0;i<size;i++) {
        		file = tables.get(i);
        		if (file.getId() == tableid) {
        			index = i;
        			break;
        		}
        	}
        	pkey = keys.get(index);
        }
        catch(Exception e) {
        	throw new NoSuchElementException();
        }
        return pkey;
    }

    public Iterator<Integer> tableIdIterator() {
        Integer tableIds[] = new Integer[tables.size()];
        int i;
        for (i=0; i<tables.size(); i++) {
        	DbFile file = tables.get(i);
        	int id = file.getId();
        	tableIds[i] = (Integer) id;
        }
        Iterator<Integer> it = Arrays.asList(tableIds).iterator();
        return it;
    }

    public String getTableName(int id) {
        int i;
        int size = tables.size();
        int index = -1;
    	DbFile file = null;
    	String name;
        if (size==0) {
        	throw new NoSuchElementException();
        }
        try {
        	for (i=0;i<size;i++) {
        		file = tables.get(i);
        		if (file.getId() == id) {
        			index = i;
        			break;
        		}
        	}
        	name = names.get(index);
        }
        catch(Exception e) {
        	throw new NoSuchElementException();
        }
        return name;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t + (primaryKey.equals("")? "":(" key is " + primaryKey)));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;


public class Page  implements Serializable{
    private String tableName ;
    private Vector<Record> pagevector;
    private int pageNum;
    private Object Minvalue;
    private Object Maxvalue;
    private Vector indexVector ;
    public void setMinvalue(Object minvalue) {
        Minvalue = minvalue;
    }
    public void setMaxvalue(Object maxvalue) {
        Maxvalue = maxvalue;
    }
    public Object getMinvalue() {
        return Minvalue;
    }
    public Object getMaxvalue() {
        return Maxvalue;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public int getPageNum() {
        return pageNum;
    }
    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }
    public Vector getPagevector() {
        return pagevector;
    }
    public void setPagevector(Vector pagevector) {
        this.pagevector = pagevector;
    }

    public void setIndexVector(Vector indexVector) {
        this.indexVector = indexVector;
    }
    public Vector getIndexVector() {
        return indexVector;
    }
    public Page(String tableName,int number) {
        this.tableName=tableName;

        this.pageNum=number;
        pagevector=new Vector();
        indexVector=new Vector();

    }
    public static void main(String[] args) {

        Page p = new Page("salwa",0);
        Vector v= new Vector();
        v.add(1);
        v.add(2);

        Vector f=new Vector();
        f=v;
        p.setIndexVector(v);
        //System.out.println(p.getIndexVector());
        //v.add(3);
        //p.getIndexVector().add(3);
        //p.setIndexVector(p.getIndexVector());
        System.out.println(f);












    }
}

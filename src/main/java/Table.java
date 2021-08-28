
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String tableName;
    private String ClusteringKey;
    private Hashtable<String, String> colNameType;
    private Hashtable<String, String> colNameMin;
    private Hashtable<String, String> colNameMax;
    private Vector<Page> vectorofpages;
    private int pagescounter;
    private int recordcounter;
    private Vector minPerPage= new Vector();
    private Vector recordsintable= new Vector();
    private  Vector<GridIndex> indicesInTable = new Vector<GridIndex>();
    public int getNextGridNum() {
        return nextGridNum;
    }

    public void setNextGridNum(int nextGridNum) {
        this.nextGridNum = nextGridNum;
    }

    private int nextGridNum=0;

    public Vector<GridIndex> getIndicesInTable() {
        return indicesInTable;
    }

    public void setIndicesInTable(Vector<GridIndex> indicesInTable) {
        this.indicesInTable = indicesInTable;
    }

    public Vector getMinPerPage() {
        return minPerPage;
    }
    public void setMinPerPage(Vector minPerPage) {
        this.minPerPage = minPerPage;
    }
    public int getPagescounter() {
        return pagescounter;
    }
    public void setPagescounter(int pagescounter) {
        this.pagescounter = pagescounter;
    }
    public int getRecordcounter() {
        return recordcounter;
    }
    public void setRecordcounter(int recordcounter) {
        this.recordcounter = recordcounter;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public String getClusteringKey() {
        return ClusteringKey;
    }
    public void setClusteringKey(String clusteringKey) {
        ClusteringKey = clusteringKey;
    }
    public Hashtable<String, String> getColNameType() {
        return colNameType;
    }
    public void setColNameType(Hashtable<String, String> colNameType) {
        this.colNameType = colNameType;
    }
    public Hashtable<String, String> getColNameMin() {
        return colNameMin;
    }
    public void setColNameMin(Hashtable<String, String> colNameMin) {
        this.colNameMin = colNameMin;
    }
    public Hashtable<String, String> getColNameMax() {
        return colNameMax;
    }
    public void setColNameMax(Hashtable<String, String> colNameMax) {
        this.colNameMax = colNameMax;
    }
    public Vector<Page> getVectorofpages() {
        return vectorofpages;
    }
    public void setVectorofpages(Vector<Page> vectorofpages) {
        this.vectorofpages = vectorofpages;
    }
    public Table(String tableName,String ClusteringKey,Hashtable<String,String> colNameType,
                 Hashtable<String,String> colNameMin,Hashtable<String,String> colNameMax) {
        this.tableName=tableName;
        this.ClusteringKey=ClusteringKey;
        this.colNameMin=colNameMin;
        this.colNameMax=colNameMax;
        this.colNameType=colNameType;
        vectorofpages = new Vector<Page>();
        this.recordsintable = new Vector();
        pagescounter=0;



    }
    public Vector getRecordsintable() {
        return recordsintable;
    }
    public void setRecordsintable(Vector recordsintable) {
        this.recordsintable = recordsintable;
    }
    @Override
    public String toString() {
        return "Table [tableName=" + tableName + ", ClusteringKey=" + ClusteringKey + ", colNameType=" + colNameType
                + ", colNameMin=" + colNameMin + ", colNameMax=" + colNameMax + ", vectorofpages=" + vectorofpages
                + ", pagescounter=" + pagescounter + "]";
    }

}

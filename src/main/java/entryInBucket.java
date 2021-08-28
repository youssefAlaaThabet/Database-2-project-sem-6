import java.io.Serializable;
import java.util.Hashtable;

public class entryInBucket implements Serializable {
    private int rowNum;
    private Hashtable rowValues;
    public entryInBucket(int rowNum,Hashtable rowValues){
        this.rowNum=rowNum;
        this.rowValues=rowValues;
    }

    public int getRowNum() {
        return rowNum;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    public Hashtable getRowValues() {
        return rowValues;
    }

    public void setRowValues(Hashtable rowValues) {
        this.rowValues = rowValues;
    }
}

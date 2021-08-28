import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {
    private int BucketNumber;
    private Vector<String> PageNames;
    private Vector<Vector<entryInBucket>> RowEntriesInPage= new Vector<>();
    private  Vector<Bucket> overFlowBuckets = new Vector<Bucket>();

    public Vector<Bucket> getOverFlowBuckets() {
        return overFlowBuckets;
    }

    public void setOverFlowBuckets(Vector<Bucket> overFlowBuckets) {
        this.overFlowBuckets = overFlowBuckets;
    }

    public Bucket(int BucketNumber){
        this.BucketNumber=BucketNumber;

    }
    public Vector<Vector<entryInBucket>> getRowEntriesInPage() {
        return RowEntriesInPage;
    }
    public void setRowEntriesInPage(Vector<Vector<entryInBucket>> rowEntriesInPage) {
        RowEntriesInPage = rowEntriesInPage;
    }
    public Vector<String> getPageNames() {
        return PageNames;
    }
    public void setPageNames(Vector<String> pageNames) {
        PageNames = pageNames;
    }
    public int getBucketNumber() {
        return BucketNumber;
    }
    public void setBucketNumber(int bucketNumber) {
        BucketNumber = bucketNumber;
    }


}

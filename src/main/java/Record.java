

import java.io.Serializable;
import java.util.Hashtable;

public class Record implements Serializable {
    private String clusteringkey;
    private Hashtable<String,Object> fullrecord=new Hashtable();

    public Record(String clusteringkey) {
        this.clusteringkey=clusteringkey;
    }

    public String getClusteringkey() {
        return clusteringkey;
    }

    public void setClusteringkey(String clusteringkey) {
        this.clusteringkey = clusteringkey;
    }

    public Hashtable<String, Object> getFullrecord() {
        return fullrecord;
    }

    public void setFullrecord(Hashtable<String, Object> fullrecord) {
        this.fullrecord = fullrecord;
    }

}

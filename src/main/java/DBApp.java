import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp implements DBAppInterface {
    FileWriter csvWriter;
    Vector TableNames = new Vector();
    Vector<Table> tables = new Vector<Table>();
    Vector config = Configure();
    int maxPerBucket= (int) config.get(1);
    @Override
    public void init() {
        if (!(new File("src\\main\\resources\\data\\Pages")).exists()) {
            File file = new File("src\\main\\resources\\data\\Pages");
            boolean bool = file.mkdir();
        }
        if (!(new File("src\\main\\resources\\data\\Buckets")).exists()) {
            File file = new File("src\\main\\resources\\data\\Buckets");
            boolean bool = file.mkdir();
        }
        if (!(new File("src\\main\\resources\\data\\tableNames")).exists()) {

            //serializing TableNames
            serialize("src\\main\\resources\\data\\tableNames",TableNames);

        }
        if ((new File("src\\main\\resources\\data\\tables")).exists()) {
            //desrilizing tables
            tables =desertable("src\\main\\resources\\data\\tables");
        }
        if ((new File("src\\main\\resources\\data\\tableNames")).exists()) {
            //deserializing tablenames
            TableNames=desertable("src\\main\\resources\\data\\tableNames");

        }
        if (!(new File("src\\main\\resources\\metadata.csv")).exists()) {
            try {
                csvWriter = new FileWriter("src\\main\\resources\\metadata.csv");
                //csvWriter.write("Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max \n");
                csvWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public void bucketOverFlow(GridIndex g,Table t,entryInBucket e,Bucket b,int pNum){
        if(b.getOverFlowBuckets().size()==0){
            int j = g.getNextBucket();
            Bucket b1 = new Bucket(g.getNextBucket());
            g.setNextBucket(g.getNextBucket() + 1);
            Vector x =new Vector();
            x.add(t.getTableName() + pNum+"");
            b1.setPageNames(x);
            x=null;
            int ind = b1.getPageNames().indexOf(t.getTableName() + pNum);
            Vector<Vector<entryInBucket>> y = b1.getRowEntriesInPage();
            Vector<entryInBucket> v = new Vector<entryInBucket>();
            v.add(e);
            y.insertElementAt(v, ind);
            v=null;
            //y.get(ind).add(e);
            b1.setRowEntriesInPage(y);
            y=null;
            Vector k =  b.getOverFlowBuckets();
            k.add(b1);
            b.setOverFlowBuckets(k);
            k=null;
            //serialize bucket
            serializeBucket("src\\main\\resources\\data\\Buckets\\" + t.getTableName() + j + ".ser", b1);
        }
        else {
            for (int i = 0; i < b.getOverFlowBuckets().size(); i++) {
                if (new File("src\\main\\resources\\data\\Buckets\\" + t.getTableName() + b.getOverFlowBuckets().get(i).getBucketNumber() + ".ser").exists()) {
                    if (b.getOverFlowBuckets().get(i).getPageNames().size() < maxPerBucket) {
                        Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + t.getTableName() + b.getOverFlowBuckets().get(i).getBucketNumber() + ".ser");
                        Vector x =  b1.getPageNames();
                        x.add(t.getTableName() + pNum+"");
                        b1.setPageNames(x);
                        x=null;
                        int ind = b1.getPageNames().indexOf(t.getTableName() + pNum);
                        Vector<Vector<entryInBucket>> y = b1.getRowEntriesInPage();
                        Vector<entryInBucket> v = new Vector<entryInBucket>();
                        v.add(e);
                        y.insertElementAt(v, ind);
                        v=null;
                        b1.setRowEntriesInPage(y);
                        y=null;
                        //serialize bucket
                        serializeBucket("src\\main\\resources\\data\\Buckets\\" + t.getTableName() + b.getOverFlowBuckets().get(i).getBucketNumber() + ".ser", b1);
                        break;
                    }
                } else {
                    int j = g.getNextBucket();
                    Bucket b1 = new Bucket(g.getNextBucket());
                    g.setNextBucket(g.getNextBucket() + 1);
                    Vector x = null;
                    x.add(t.getTableName() + pNum+"");
                    b1.setPageNames(x);
                    x=null;
                    int ind = b1.getPageNames().indexOf(t.getTableName() + pNum);
                    Vector<Vector<entryInBucket>> y = b1.getRowEntriesInPage();
                    Vector<entryInBucket> v = new Vector<entryInBucket>();
                    v.add(e);
                    y.insertElementAt(v, ind);
                    v=null;
                    //y.get(ind).add(e);
                    b1.setRowEntriesInPage(y);
                    y=null;
                    Vector k = b.getOverFlowBuckets();
                    k.add(b1);
                    b.setOverFlowBuckets(k);
                    //serialize bucket
                    serializeBucket("src\\main\\resources\\data\\Buckets\\" + t.getTableName() + j + ".ser", b1);
                    break;
                }
            }
        }
    }
    @Override
    public void createTable(String tableName, String clusteringKey,
                            Hashtable<String, String> colNameType,
                            Hashtable<String, String> colNameMin,
                            Hashtable<String, String> colNameMax) throws DBAppException {
        //deserializing tablenames
        TableNames=desertable("src\\main\\resources\\data\\tableNames");

        if (new File("src\\main\\resources\\data\\tables").exists()) {
            //deserializing tables
            tables =desertable("src\\main\\resources\\data\\tables");
        }
        Enumeration names;
        String str;
        String flag = "FALSE";
        names = colNameType.keys();
        if (TableNames.contains(tableName)) {
            throw new DBAppException();
        } else {
            TableNames.add(tableName);
            tables.add(new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
            while (names.hasMoreElements()) {
                str = (String) names.nextElement();
                if (clusteringKey == str) {
                    flag = "TRUE";
                }
                try {
                    File metadataFile = new File("src\\main\\resources\\metadata.csv");
                    FileWriter writer = new FileWriter(metadataFile, true);
                    writer.write(tableName + "," + str + "," + colNameType.get(str) + "," + flag + ","
                            + "FALSE ," + colNameMin.get(str) + "," + colNameMax.get(str) + "\n");
                    writer.close();
                    flag = "FALSE";
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //serializing tables
            serialize("src\\main\\resources\\data\\tables",tables);

            //serializing TableNames
            serialize("src\\main\\resources\\data\\tableNames",TableNames);

        }
    }
    @Override
    public void createIndex(String tableName, String[] columnNames)
            throws DBAppException {
        TableNames = desertable("src\\main\\resources\\data\\tableNames");
        tables = desertable("src\\main\\resources\\data\\tables");
        int m = TableNames.indexOf(tableName);
        Table t = (Table) tables.get(m);
        Vector columnnames=new Vector();
        columnnames.addAll(Arrays.asList(columnNames));

        if(!TableNames.contains(tableName)){                                 //CHECK IF TABLE IS AVAILABLE
            throw new DBAppException();
        }

        else {

            Enumeration<String> ke = t.getColNameType().keys();              //CHECK IF COLUMNS ARE THE SAME IN THE TABLE
            Vector arr= new Vector();
            Vector arr1= new Vector();
            while(ke.hasMoreElements()){
                arr.add(ke.nextElement());
            }
            for(int r=0;r<columnNames.length;r++){
                arr1.add(columnNames[r]);
            }
            if (!arr.containsAll(arr1)){
                throw new DBAppException();

            }
            for(int e=0;e<columnNames.length-1;e++)                          //CHECK FOR DUPLICATES
            {

                for(int q=e+1;q<columnNames.length;q++)
                {
                    if(columnNames[e].equals(columnNames[q]))
                    {
                        throw new DBAppException();
                    }
                }
            }
            //CHECK IF INDEX ALREADY CREATED
            BufferedReader csvReader = null;
            try {
                csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
            } catch (FileNotFoundException e2) {
                e2.printStackTrace();
            }
            try {
                File myObj = new File("src\\main\\resources\\metadata.csv");
                Scanner myReader = new Scanner(myObj);
                String alldata="";
                while (myReader.hasNextLine()) {
                    String row = myReader.nextLine();
                    String[] data = row.split(",");
                    if (!data[0].equals(tableName)){//csv checks
                        alldata+=row+"\n";
                        continue;

                    }else{
                        if(columnnames.contains(data[1])){
                            alldata+=data[0]+","+data[1]+","+data[2]+","+data[3]+","+"TRUE"+","+data[5]+","+data[6]+"\n" ;
                            continue;
                        }
                        else{
                            alldata+=row+"\n";
                            continue;
                        }
                    }
                }
                try {
                    File metadataFile = new File("src\\main\\resources\\metadata.csv");
                    FileWriter writer = new FileWriter(metadataFile, false);
                    writer.write(alldata);
                    writer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
        Vector ranges = new Vector();
        GridIndex g = new GridIndex(columnNames,tableName);
        g.setGridNum(t.getNextGridNum());
        t.setNextGridNum(t.getNextGridNum()+1);


        // Flooding the index (table contain data before creating index)
        int recordscount=t.getRecordsintable().size();
        if(recordscount>0){
            for(int i = 0; i < t.getVectorofpages().size(); i++ ){
                int pNum = t.getVectorofpages().get(i).getPageNum();
                Page p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + pNum + ".ser");
                for(int j = 0; j < p.getIndexVector().size(); j++){
                    Record r= new Record(t.getClusteringKey());
                    r.setFullrecord((Hashtable<String, Object>) p.getPagevector().get(j));
                    int [] concatenate=new int[columnNames.length];
                    for(int k =0;k<columnNames.length;k++){
                        Object reqValue=r.getFullrecord().get(columnNames[k]);
                        int indexingrid = getIndexinDivVec(g,columnNames[k],reqValue);
                        concatenate[k]=indexingrid;
                    }
                    int bucketnumber =concatenateindex(concatenate,g.getVectorofColumnDivs());
                    //   System.out.println("thabota koko");
                    //  System.out.println(bucketnumber);
                    if((g.getRefBucketArray().get(bucketnumber)!=null)){
                        //deser bucket
                        Bucket b = deserBucket("src\\main\\resources\\data\\Buckets\\"+g.getRefBucketArray().get(bucketnumber)+".ser");
                        if(!b.getPageNames().contains(tableName+pNum)) {
                            Vector x =b.getPageNames();
                            x.add(tableName + pNum);
                            b.setPageNames(x);
                            x=null;
                            int ind = b.getPageNames().indexOf(tableName+pNum);
                            Vector<Vector<entryInBucket>> y = b.getRowEntriesInPage();
                            Hashtable h = new Hashtable();
                            for(int l=0;l<columnNames.length;l++){
                                h.put(r.getClusteringkey(),r.getFullrecord().get(t.getClusteringKey()));
                                if(!(columnNames[l]==r.getClusteringkey())) {
                                    h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                }
                            }
                            entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())),h);
                            Vector<entryInBucket> v= new Vector<entryInBucket>();
                            v.add(e);
                            y.insertElementAt(v,ind);
                            v=null;
                            //y.get(ind).add(e);
                            b.setRowEntriesInPage(y);
                            y=null;
                            //serialize bucket
                            serializeBucket("src\\main\\resources\\data\\Buckets\\"+g.getRefBucketArray().get(bucketnumber)+".ser",b);
                        }
                        //pageNames contain the req page name
                        else {
                            int ind = b.getPageNames().indexOf(tableName+pNum);
                            Vector<Vector<entryInBucket>> y = b.getRowEntriesInPage();
                            Hashtable h = new Hashtable();
                            for(int l=0;l<columnNames.length;l++){
                                if((r.getFullrecord().get(columnNames[l]))!=null) {
                                    h.put(r.getClusteringkey(),r.getFullrecord().get(t.getClusteringKey()));
                                    if(!(columnNames[l]==r.getClusteringkey())) {
                                        h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                    }
                                }

                            }
                            entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())),h);
                            y.get(ind).add(e);
                            b.setRowEntriesInPage(y);
                            y=null;
                            //serialize bucket
                            serializeBucket("src\\main\\resources\\data\\Buckets\\"+g.getRefBucketArray().get(bucketnumber)+".ser",b);
                        }
                    } //if bucket is not found
                    else {
                        Bucket b = new Bucket(g.getNextBucket());
                        int num = g.getNextBucket();
                        Vector k = g.getRefBucketArray();
                        String a=g.getGridNum()+g.getTablename()+b.getBucketNumber();
                        k.setElementAt(a,bucketnumber);
                        g.setRefBucketArray(k);
                        num+=1;
                        g.setNextBucket(num);
                        Vector x = new Vector();
                        x.add(tableName+pNum);
                        b.setPageNames(x);
                        x=null;
                        Vector<Vector<entryInBucket>> y = new Vector<Vector<entryInBucket>>();
                        Hashtable h = new Hashtable();
                        for(int l=0;l<columnNames.length;l++){
                            if((r.getFullrecord().get(columnNames[l]) !=null)) {
                                h.put(r.getClusteringkey(),r.getFullrecord().get(t.getClusteringKey()));
                                if(!(columnNames[l]==r.getClusteringkey())) {
                                    h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                }
                            }
                        }
                        entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())),h);
                        Vector <entryInBucket>xm = new Vector <entryInBucket>();
                        xm.add(e);
                        y.add(xm);
                        b.setRowEntriesInPage(y);
                        y=null;
                        serializeBucket("src\\main\\resources\\data\\Buckets\\"+g.getRefBucketArray().get(bucketnumber)+".ser",b);

                    }

                }}

        }

        Vector<GridIndex> tt= t.getIndicesInTable();
//        x=t.getIndicesInTable();
        tt.add(g);
        t.setIndicesInTable(tt);
        tt=null;
        serialize("src\\main\\resources\\data\\tables", tables);


    }

    public int getIndexinDivVec(GridIndex g, String columnName, Object reqValue){
        int vectorNum = -1;

        for(int z=0;z<g.getVectorofColumnDivs().size();z++){

            if(g.getVectorofColumnDivs().get(z).getColumnname().equals(columnName)){
                vectorNum=z;
                break;
            }
        }
        if((reqValue!=null)&&!g.getVectorofColumnDivs().get(vectorNum).getType().equals(reqValue.getClass().getName())){
            return -1;
        }
        int x = g.getVectorofColumnDivs().get(vectorNum).getDivisionVector().size();
        for(int i=0;i<x;i++) {
            //line 261 operator should be ">"
            if (reqValue != null && compareObjects(reqValue, ((minmaxstructure) (g.getVectorofColumnDivs().get(vectorNum).getDivisionVector().get(i))).getMin()) >= 0 && compareObjects(reqValue, ((minmaxstructure) (g.getVectorofColumnDivs().get(vectorNum).getDivisionVector().get(i))).getMax()) < 0) {
                return i;
            } else {
                if (reqValue == null) {
                    return 10;
                }
            }
        }
        return vectorNum;
    }
    //[0,2,5]
    //make method takes parameter array of indexes to concatenate
    public int concatenateindex(int[] indices,Vector <customizedvector> vec){
        int x=0;
        int k=1;
        for(int i=0;i<indices.length;i++) {

            //for(int j=i;j<v.size()-1;j++) {
            k=1*indices[i];
            for(int z=i+1;z<vec.size();z++) {
                k=k*(vec.get(z).getDivisionVector().size());
            }
            x+=k;
            //}Vector <customizedvector> vectorofColumnDivs

        }

        return x;
    }

    public void updateIndexAfterInsert(Table t,Record r, String tableName,int pNum){
        //TableNames = desertable("src\\main\\resources\\data\\tableNames");
       // tables = desertable("src\\main\\resources\\data\\tables");
        if(t.getIndicesInTable().size()!=0) {
            Page p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + pNum + ".ser");
            //int loc = TableNames.indexOf(tableName);
            // Table t = tables.get(loc);
            Vector<GridIndex> tmp = new Vector<GridIndex>();
            for (int i = 0; i < t.getIndicesInTable().size(); i++) {
                GridIndex g = t.getIndicesInTable().get(i);
                String[] columnNames = g.getColumnnames();
                int[] divsindexes = new int[columnNames.length];
                for (int j = 0; j < columnNames.length; j++) {
                    String colname = (String) columnNames[j];
                    int indexvalue = getIndexinDivVec(g, colname, r.getFullrecord().get(colname));
                    divsindexes[j] = indexvalue;
                }
                int bucketnumber = concatenateindex(divsindexes, g.getVectorofColumnDivs());
                if (!(g.getRefBucketArray().get(bucketnumber) == null)) {
                    //deser bucket

                    Bucket b = deserBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser");
                    if (b.getRowEntriesInPage().size() < maxPerBucket) {
                        if (!b.getPageNames().contains(tableName + pNum)) {
                            Vector x = b.getPageNames();
                            x.add(tableName + pNum);
                            b.setPageNames(x);
                            x=null;
                            int ind = b.getPageNames().indexOf(tableName + pNum);
                            Vector<Vector<entryInBucket>> y = b.getRowEntriesInPage();
                            Hashtable h = new Hashtable();
                            for (int l = 0; l < columnNames.length; l++) {
                                h.put(r.getClusteringkey(), r.getFullrecord().get(t.getClusteringKey()));
                                if (!(columnNames[l] == r.getClusteringkey())) {
                                    h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                }
                            }
                            entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())), h);
                            Vector<entryInBucket> v = new Vector<entryInBucket>();
                            v.add(e);
                            y.insertElementAt(v, ind);
                            b.setRowEntriesInPage(y);
                            v=null;
                            y=null;
                            //serialize bucket
                            serializeBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser", b);
                        }
                        //pageNames contain the req page name
                        else {
                            int ind = b.getPageNames().indexOf(tableName + pNum);
                            Vector<Vector<entryInBucket>> y = b.getRowEntriesInPage();
                            Hashtable h = new Hashtable();
                            for (int l = 0; l < columnNames.length; l++) {
                                if ((r.getFullrecord().get(columnNames[l])) != null) {
                                    h.put(r.getClusteringkey(), r.getFullrecord().get(t.getClusteringKey()));
                                    if (!(columnNames[l] == r.getClusteringkey())) {
                                        h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                    }
                                }

                            }
                            entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())), h);
                            y.get(ind).add(e);
                            b.setRowEntriesInPage(y);
                            e=null;
                            y=null;
                            //serialize bucket
                            serializeBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser", b);
                        }
                    } else {
                        Hashtable h = new Hashtable();
                        for (int l = 0; l < columnNames.length; l++) {
                            if ((r.getFullrecord().get(columnNames[l])) != null) {
                                h.put(r.getClusteringkey(), r.getFullrecord().get(t.getClusteringKey()));
                                if (!(columnNames[l] == r.getClusteringkey())) {
                                    h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                                }
                            }
                        }
                        entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())), h);
                        bucketOverFlow(g, t, e, b, pNum);
                    }
                } //if bucket is not found
                else {
                    Bucket b = new Bucket(g.getNextBucket());
                    int num = g.getNextBucket();

                    Vector x = new Vector();
                    x.add(tableName + pNum + "");
                    b.setPageNames(x);
                    x=null;
                    Vector<Vector<entryInBucket>> y = new Vector<Vector<entryInBucket>>();
                    Hashtable h = new Hashtable();
                    for (int l = 0; l < columnNames.length; l++) {
                        if ((r.getFullrecord().get(columnNames[l]) != null)) {
                            h.put(r.getClusteringkey(), r.getFullrecord().get(t.getClusteringKey()));
                            if (!(columnNames[l] == r.getClusteringkey())) {
                                h.put(columnNames[l], r.getFullrecord().get(columnNames[l]));
                            }
                        }
                    }
                    entryInBucket e = new entryInBucket(p.getIndexVector().indexOf(r.getFullrecord().get(r.getClusteringkey())), h);
                    Vector<entryInBucket> xm = new Vector<entryInBucket>();
                    xm.add(e);
                    y.add(xm);
                    b.setRowEntriesInPage(y);
                    y=null;
                    //add the new bucket to the bucket array
                /*int index=-1;
                boolean flag=true;
                for (int u =0;u<t.getIndicesInTable().size();u++){
                    for (int u2 =0;u2<t.getIndicesInTable().get(u).getColumnnames().length;u2++){
                        for (int u3 =0;u3<columnNames.length;u3++){
                            if(columnNames[u3]!=t.getIndicesInTable().get(u).getColumnnames()[u2]){
                                flag=false;
                                break;
                            }
                        }

                    }

                    if (flag){
                        index=u;

                    }
                }*/
                    Vector newgrid = g.getRefBucketArray();

                    newgrid.setElementAt(g.getGridNum() + tableName + num, bucketnumber);
                    g.setRefBucketArray(newgrid);
                    newgrid=null;
                    num += 1;
                    g.setNextBucket(num);
                    // serialize bucket
                    serializeBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser", b);

                }
                tmp.add(g);

            }
            t.setIndicesInTable(tmp);
            tmp=null;
            //tables.removeElementAt(loc);
            //tables.insertElementAt(t,loc);
            serialize("src\\main\\resources\\data\\tables", tables);
        }
    }

    @Override
    public void insertIntoTable(String tableName,
                                Hashtable<String, Object> colNameValue) throws DBAppException {
        int maxPerPage = (int) config.get(0);
        //deserializing tablenames
        TableNames=desertable("src\\main\\resources\\data\\tableNames");

        //deserializing tables
        tables =desertable("src\\main\\resources\\data\\tables");

        if (TableNames.contains(tableName)) {
            String str;
            int i = TableNames.indexOf(tableName);
            Table t = (Table) tables.get(i);
            if(colNameValue.get(t.getClusteringKey())==null){
                throw new DBAppException();
            }
            else {
                if (t.getRecordcounter() == 0) {
                    Enumeration<String> k = t.getColNameType().keys();
                    Enumeration<String> a = colNameValue.keys();
                    Vector arr = new Vector();
                    Vector arr1 = new Vector();
                    while (k.hasMoreElements()) {
                        arr.add(k.nextElement());
                    }
                    while (a.hasMoreElements()) {
                        arr1.add(a.nextElement());
                    }
                    if (!arr.containsAll(arr1)) {
                        throw new DBAppException();
                    }
                    Page p = new Page(tableName, 0);
                    t.getVectorofpages().add(p);
                    String cluster = t.getClusteringKey();
                    Record r = new Record(cluster);
                    r.setFullrecord(colNameValue);

                    Enumeration keys = colNameValue.keys();
                    BufferedReader csvReader = null;

                    try {
                        csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
                    } catch (FileNotFoundException e2) {
                        e2.printStackTrace();
                    }
                    try {
                        File myObj = new File("src\\main\\resources\\metadata.csv");
                        Scanner myReader = new Scanner(myObj);
                        while (myReader.hasNextLine()) {
                            String row = myReader.nextLine();
                            String[] data = row.split(",");
                            if (!data[0].equals(tableName))//csv checks
                                continue;
                            if ( data[2].equals("java.util.Date")) {
                                if(colNameValue.containsKey(data[1])){

                                    SimpleDateFormat originalFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
                                    SimpleDateFormat targetFormat = new SimpleDateFormat("YYYY-MM-DD");
                                    java.util.Date date =originalFormat.parse(colNameValue.get(data[1]).toString());
                                    String formattedDate = targetFormat.format(date);

                                    //Date date1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH).parse(colNameValue.get(data[1]).toString());
                                    Date date1 = new SimpleDateFormat("YYYY-MM-DD").parse(formattedDate);
                                    colNameValue.replace(data[1], date1);
                                }
                                continue;
                            }
                            if ( data[2].equals("java.lang.Double")) {
                                String s = colNameValue.get(data[1]).toString();
                                double d = Double.parseDouble(s);
                                colNameValue.replace(colNameValue.get(data[1]).toString(), d);
                            }
                            if ((colNameValue.get(data[1])!=null)&&!colNameValue.get(data[1]).getClass().getName().equals(data[2])) {
                                throw new DBAppException();
                            }
                            if ((colNameValue.get(data[1])!=null)&&!(((colNameValue.get(data[1])).toString().compareTo(data[5])) >= 0) && !((colNameValue.get(data[1])).toString().compareTo(data[6]) <= 0)) {
                                throw new DBAppException();
                            }
                        }
                        myReader.close();
                    } catch (FileNotFoundException | ParseException e) {
                        e.printStackTrace();
                    }
                    while (keys.hasMoreElements()) {
                        str = (String) keys.nextElement();
                        if (!t.getColNameType().containsKey(str) || !colNameValue.containsKey(cluster) || colNameValue.get(cluster) == "") {//checking clusteringkey and checkin available column type
                            throw new DBAppException();
                        } else {
                            r.setFullrecord(colNameValue);
                            insert(t, r, 0, 0);

                            break;
                        }
                    }
                    Vector id = p.getIndexVector();
                    id.add(colNameValue.get(cluster));
                    p.setIndexVector(id);
                    id=null;
                    p.setMinvalue(colNameValue.get(cluster));
                    Vector minvector =  t.getMinPerPage();
                    minvector.add(colNameValue.get(cluster));
                    t.setMinPerPage(minvector);
                    minvector=null;
                    //updating index
                    serialize("src\\main\\resources\\data\\tables",tables);
                    updateIndexAfterInsert(t,r,tableName,0);
                } else {
                    Enumeration<String> k = t.getColNameType().keys();
                    Enumeration<String> a = colNameValue.keys();
                    Vector arr = new Vector();
                    Vector arr1 = new Vector();
                    while (k.hasMoreElements()) {
                        arr.add(k.nextElement());
                    }
                    while (a.hasMoreElements()) {
                        arr1.add(a.nextElement());
                    }
                    if (!arr.containsAll(arr1)) {
                        throw new DBAppException();
                    }
                    String cluster = t.getClusteringKey();
                    Enumeration keys = colNameValue.keys();
                    BufferedReader csvReader = null;
                    Record r = new Record(cluster);
                    r.setFullrecord(colNameValue);
                    Vector vn = t.getRecordsintable();
                    boolean flag=true;
                    if(vn.contains(r.getFullrecord().get(r.getClusteringkey()))){
                        flag=false;
                        throw new DBAppException();
                    }

                    if(flag==false){
                        return;
                    }
                    vn=null;
                    int u1 = getPageNumber(t, r);
                    int u5 = u1+1;
                    //deserializing page
                    Page p=null;
                    if(new File("src\\main\\resources\\data\\Pages\\" + tableName + u1 + ".ser").exists())
                        p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + u1 + ".ser");

                    try {
                        csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
                    } catch (FileNotFoundException e2) {
                        e2.printStackTrace();
                    }
                    try {
                        File myObj = new File("src\\main\\resources\\metadata.csv");
                        Scanner myReader = new Scanner(myObj);
                        while (myReader.hasNextLine()) {
                            String row = myReader.nextLine();
                            String[] data = row.split(",");
                            if (data[0].equals(tableName))//csv checks
                                continue;
                            if ( data[2].equals("java.util.Date")) {
                                if(colNameValue.containsKey(data[1])){
                                    Date date1 = new SimpleDateFormat("YYYY-MM-DD").parse(colNameValue.get(data[1]).toString());
                                    colNameValue.replace(data[1], date1);
                                    continue;
                                }
                            }
                           else if (data[2].equals("java.lang.Double")) {
                                if(colNameValue.containsKey(data[1])){
                                    String s = (String) colNameValue.get(data[1]).toString();
                                    double d = Double.parseDouble(s);
                                    colNameValue.replace(data[1], d);
                                }

                            }
                          //  System.out.println(data[1]);
                            Object o = colNameValue.get(data[1]);
                            if (o!=null && !o.getClass().getName().equals(data[2])) {
                                throw new DBAppException();
                            }
                            if (colNameValue.get(data[1])!=null&&!((( colNameValue.get(data[1])).toString().compareTo(data[5])) >= 0) && !(( colNameValue.get(data[1])).toString().compareTo(data[6]) <= 0)) {
                                throw new DBAppException();
                            }
                        }

                        myReader.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    while (keys.hasMoreElements()) {
                        str = (String) keys.nextElement();

                        if (!t.getColNameType().containsKey(str) || !colNameValue.containsKey(cluster)) {//checking clusteringkey and checkin available column type
                            throw new DBAppException();
                        }
                    }
                    if (p!=null && maxPerPage == p.getIndexVector().size() ) {

                        Hashtable<String, Object> insert2 = (Hashtable<String, Object>) p.getPagevector().lastElement();
                        Record r2 = new Record(t.getClusteringKey());
                        r2.setFullrecord(insert2);
                        if (compareObjects(r2.getFullrecord().get(t.getClusteringKey()), r.getFullrecord().get(t.getClusteringKey())) > 0) {
                            int u=-1;
                            //New Editt
                            if(t.getVectorofpages().size()>u5) {
                                if (!new File("src\\main\\resources\\data\\Pages\\" + tableName + t.getVectorofpages().get(u5).getPageNum() + ".ser").exists()) {
                                    Page m1 = new Page(tableName, u5);
                                    Vector vop = t.getVectorofpages();
                                    vop.insertElementAt(m1, u5);
                                    t.setVectorofpages(vop);
                                    vop = null;
                                    u = m1.getPageNum();
                                    serialize("src\\main\\resources\\data\\Pages\\" + tableName + u5 + ".ser", m1);
                                    serialize("src\\main\\resources\\data\\tables", tables);
                                } else {
                                    Page m2 = deserpage("src\\main\\resources\\data\\Pages\\" + t.getTableName() + t.getVectorofpages().get(u5).getPageNum() + ".ser");
                                    u = m2.getPageNum();
                                }
                            }
                            else {
                                Page m1 = new Page(tableName, u5);
                                Vector vop = t.getVectorofpages();
                                vop.insertElementAt(m1, u5);
                                t.setVectorofpages(vop);
                                vop=null;
                                u = m1.getPageNum();
                                serialize("src\\main\\resources\\data\\Pages\\" + tableName + u5 + ".ser", m1);
                                serialize("src\\main\\resources\\data\\tables", tables);
                            }
                            //int u  = t.getVectorofpages().get(u5).getPageNum();

                            //serialize("src\\main\\resources\\data\\Pages\\" + tableName + u + ".ser", m1);


                            if (new File("src\\main\\resources\\data\\Pages\\" + tableName + u + ".ser").exists()) {

                                if (t.getVectorofpages().get(u5).getIndexVector().size() < maxPerPage) {
                                    deleteFromTable(tableName, insert2);
                                    //desrilizing tables
                                    tables = desertable("src\\main\\resources\\data\\tables");
                                    int i2 = TableNames.indexOf(tableName);
                                    Table t2 = (Table) tables.get(i2);


                                    if (t2.getVectorofpages().get(u5).getIndexVector().size() == 0) {
                                        Vector nm = t2.getMinPerPage();
                                        nm.add(r2.getFullrecord().get(t2.getClusteringKey()));
                                        t2.setMinPerPage(nm);
                                    } else {
                                        if(compareObjects(p.getIndexVector().get(0),r.getFullrecord().get(t2.getClusteringKey()))>0) {
                                            Vector nm =  t2.getMinPerPage();
                                            Object ooo = p.getIndexVector().get(0);
                                            nm.remove(ooo);
                                            nm.insertElementAt(r.getFullrecord().get(t2.getClusteringKey()), u5);
                                            t2.setMinPerPage(nm);
                                            nm=null;
                                        }
                                    }
                                    insert(t2, r2, u, getIndexInPage(u, r2, tableName));
                                    updateIndexAfterInsert(t2,r2,tableName,u);

                                    insert(t2, r, u1, getIndexInPage(u1, r, tableName));

                                    updateIndexAfterInsert(t2,r,tableName,u1);
                                }
                                //overflow here
                                else {
                                    //int index = tables.indexOf(tableName);
                                    //Table to = tables.get(index);
                                    int overflowPageNum = t.getVectorofpages().size();// to get 3 in the example
                                    Page overflowPage = new Page(tableName, overflowPageNum);
                                    Vector newVecofPages =  t.getVectorofpages();
                                    newVecofPages.insertElementAt(overflowPage, u);
                                    t.setVectorofpages(newVecofPages);
                                    newVecofPages=null;
                                    ////check here
                                    Vector x1 = t.getMinPerPage();
                                    x1.insertElementAt(r.getFullrecord().get(r.getClusteringkey()), u);
                                    t.setMinPerPage(x1);
                                    x1=null;
                                    serialize("src\\main\\resources\\data\\Pages\\" + tableName + overflowPageNum + ".ser", overflowPage);
                                    insert(t, r, overflowPageNum, getIndexInPage(overflowPageNum, r, tableName));
                                    updateIndexAfterInsert(t,r,tableName,overflowPageNum);

                                    //a3melha getindexinPage() ???
                                    //3yzin nt2aked en el index vector w kolo mt-handle
                                }
                            } else {
                                deleteFromTable(tableName, insert2);
                                //deserializing tables
                                tables = desertable("src\\main\\resources\\data\\tables");
                                int i2 = TableNames.indexOf(tableName);
                                Table t2 = (Table) tables.get(i2);
                                Page m = new Page(tableName, u);
                                Vector vop = t2.getVectorofpages();
                                vop.add(m);
                                t2.setVectorofpages(vop);
                                vop=null;
                                Vector nm = t2.getMinPerPage();
                                nm.insertElementAt(r2.getFullrecord().get(t2.getClusteringKey()),u5);
                                t2.setMinPerPage(nm);
                                nm=null;

                                //serializing page
                                serialize("src\\main\\resources\\data\\Pages\\" + tableName + u + ".ser", m);

                                insert(t2, r2, u, getIndexInPage(u, r2, tableName));
                                updateIndexAfterInsert(t2,r2,tableName,u);
                                insert(t2, r, u1, getIndexInPage(u1, r, tableName));
                                updateIndexAfterInsert(t2,r,tableName,u1);
                            }
                        } else {
                            int u=-1;
                            if(t.getVectorofpages().size()>u5) {
                                if (!new File("src\\main\\resources\\data\\Pages\\" + tableName + u5 + ".ser").exists()) {
                                    Page m1 = new Page(tableName, u5);
                                    Vector vop = t.getVectorofpages();
                                    vop.insertElementAt(m1, u5);
                                    t.setVectorofpages(vop);
                                    vop=null;
                                    u = m1.getPageNum();
                                    serialize("src\\main\\resources\\data\\Pages\\" + tableName + u5 + ".ser", m1);
                                    serialize("src\\main\\resources\\data\\tables", tables);
                                } else {
                                    Page m2 = deserpage("src\\main\\resources\\data\\Pages\\" + t.getTableName() + u5 + ".ser");
                                    u = m2.getPageNum();
                                }

                            } else {
                                Page m1 = new Page(tableName, u5);
                                Vector vop = t.getVectorofpages();
                                vop.insertElementAt(m1, u5);
                                t.setVectorofpages(vop);
                                vop=null;
                                u = m1.getPageNum();
                                serialize("src\\main\\resources\\data\\Pages\\" + tableName + u5 + ".ser", m1);
                                serialize("src\\main\\resources\\data\\tables", tables);
                            }
                            //int u = t.getVectorofpages().get(u5).getPageNum();

                            if (new File("src\\main\\resources\\data\\Pages\\" + tableName + u + ".ser").exists()) {
                                if (t.getVectorofpages().get(u5).getIndexVector().size() < maxPerPage) {
                                    //desrilizing tables
                                    int PageNumber = u;
                                    //deserializing page
                                    p = deserpage("src\\main\\resources\\data\\Pages\\" + t.getTableName() + PageNumber + ".ser");

                                    if (p.getIndexVector().size() == 0) {
                                        Vector nm = t.getMinPerPage();
                                        nm.add(r.getFullrecord().get(t.getClusteringKey()));
                                        t.setMinPerPage(nm);
                                        nm=null;
                                    } else {
                                        if(compareObjects(p.getIndexVector().get(0),r.getFullrecord().get(t.getClusteringKey()))>0) {
                                            Vector nm =t.getMinPerPage();
                                            Object ooo = p.getIndexVector().get(0);
                                            nm.remove(ooo);
                                            nm.insertElementAt(r.getFullrecord().get(t.getClusteringKey()), u5);
                                            t.setMinPerPage(nm);
                                            nm=null;
                                        }
                                    }
                                    insert(t, r, u, getIndexInPage(u, r, tableName));
                                    updateIndexAfterInsert(t, r, tableName, u);
                                }else {
                                    int u9=t.getVectorofpages().size();
                                    //desrilizing tables
                                    Page m = new Page(tableName, u9);
                                    Vector vp = t.getVectorofpages();
                                    vp.insertElementAt(m,u9);
                                    t.setVectorofpages(vp);
                                    Vector nm = t.getMinPerPage();
                                    nm.add(r.getFullrecord().get(t.getClusteringKey()));
                                    t.setMinPerPage(nm);
                                    nm=null;
                                    vp=null;

                                    //serializing page
                                    serialize("src\\main\\resources\\data\\Pages\\" + tableName + u9 + ".ser", m);

                                    insert(t, r, u9, getIndexInPage(u9, r, tableName));
                                    updateIndexAfterInsert(t,r,tableName,u9);
                                }


                            } else {
                                //desrilizing tables
                                tables = desertable("src\\main\\resources\\data\\tables");
                                int i2 = TableNames.indexOf(tableName);
                                Table t2 = (Table) tables.get(i2);
                                Page m = new Page(tableName, u5);
                                Vector vp = t2.getVectorofpages();
                                vp.insertElementAt(m,u5);
                                t2.setVectorofpages(vp);
                                Vector nm = t2.getMinPerPage();
                                nm.add(r.getFullrecord().get(t2.getClusteringKey()));
                                t2.setMinPerPage(nm);
                                vp=null;
                                nm=null;

                                //serializing page
                                serialize("src\\main\\resources\\data\\Pages\\" + tableName + u + ".ser", m);

                                insert(t2, r, u, getIndexInPage(u, r, tableName));
                                updateIndexAfterInsert(t2,r,tableName,u);
                            }
                        }
                    } else {
                        //desrilizing tables
                        r.setFullrecord(colNameValue);
                        insert(t, r, u1, getIndexInPage(u1, r, tableName));
                        updateIndexAfterInsert(t,r,tableName,u1);
                    }
                }
            }
            //serializing tables
            serialize("src\\main\\resources\\data\\tables",tables);

            //serializing TableNames
            serialize("src\\main\\resources\\data\\tableNames",TableNames);

        }
        else{
            throw new DBAppException();

        }
    }
    public  int getPageNumber(Table t,Record r){
        Vector k =  t.getMinPerPage();
        String cluster = t.getClusteringKey();
        Hashtable<String, Object> hash = r.getFullrecord();
        Object key = hash.get(cluster);
        if (k.contains(key)) {
            return k.indexOf(key);
        } else {

            k.add(key);
            sorting(k);
            int h=t.getRecordcounter()+1;
            int pageNumber;


            if(t.getMinPerPage().indexOf(( r.getFullrecord()).get(t.getClusteringKey()))-1>=0){
                pageNumber=t.getMinPerPage().indexOf(( r.getFullrecord()).get(t.getClusteringKey()))-1;
            }
            else{
                pageNumber=0;
            }
            if(!k.isEmpty()) {
                k.removeElementAt(pageNumber+1);
                t.setMinPerPage(k);
            }
            //serializing tables
            k=null;
            serialize("src\\main\\resources\\data\\tables",tables);
             if(t.getVectorofpages().size()>pageNumber)
            return t.getVectorofpages().get(pageNumber).getPageNum();
             else
                 return t.getVectorofpages().get(pageNumber-1).getPageNum();
        }}
    @Override
    public void deleteFromTable(String tableName,
                                Hashtable<String, Object> columnNameValue) throws DBAppException {

        //deserializing tablenames
        TableNames=desertable("src\\main\\resources\\data\\tableNames");
        Vector del = new Vector();
        if(TableNames.contains(tableName)) {
            Page p= null;
            String str;
            //desrilizing tables
            tables =desertable("src\\main\\resources\\data\\tables");
            int i = TableNames.indexOf(tableName);
            Table t = (Table)tables.get(i);
            if (columnNameValue.containsKey(t.getClusteringKey())){
                Hashtable h;
                String cluster=t.getClusteringKey();
                Record r=new Record(cluster);
                r.setFullrecord(columnNameValue);
                Vector che3 = new Vector();
                Vector che4 = new Vector();
                Vector che5 = new Vector();
                Enumeration names;
                names=columnNameValue.keys();
                String stri;
                int PageNumber =-1;
                for(int m=0;m<t.getVectorofpages().size();m++){
                    for(int g=0;g<t.getVectorofpages().get(m).getPagevector().size();g++){
                        if(compareObjects(t.getVectorofpages().get(m).getIndexVector().get(g),r.getFullrecord().get(r.getClusteringkey()))==0){
                            PageNumber=m;
                        }
                    }
                }
                while(names.hasMoreElements()) {
                    stri=(String)names.nextElement();

                    che3.addElement(stri);
                    che4.addElement(columnNameValue.get(stri));
                }
                //deserializing page
                if(new File("src\\main\\resources\\data\\Pages\\"+tableName +PageNumber+".ser").exists()){
                p = deserpage("src\\main\\resources\\data\\Pages\\"+tableName +PageNumber+".ser");
                Object o= r.getFullrecord().get(cluster);
                //for(int i=0)
                int arg0 = p.getIndexVector().indexOf(o);
                int arg1 = t.getRecordsintable().indexOf(o);
                if(arg0==-1)
                    return;
                Hashtable ky= new Hashtable <String,Object>();

                ky =(Hashtable) p.getPagevector().get(arg0);//arg0 ehy 1
                for (int ii=0;ii<che3.size();ii++){
                    che5.addElement(ky.get(che3.get(ii)));

                }
                if(che4.equals(che5)){
                    Vector f=  t.getRecordsintable();
                    f.removeElementAt(arg1);
                    t.setRecordsintable(f);
                    f=null;
                    Vector s=  p.getIndexVector();
                    //EDITTTT
                    del.add(p.getIndexVector().get(arg0));
                    s.removeElementAt(arg0);
                    //s.setElementAt(null,arg0);
                    p.setIndexVector(s);
                    s=null;
                    Vector re=  p.getPagevector();
                    //re.setElementAt(null,arg0);
                     h = (Hashtable) p.getPagevector().get(arg0);
                    //h=  ttt.getFullrecord();

                    re.removeElementAt(arg0);



                    p.setPagevector(re);
                    re=null;
                    t.setRecordcounter(t.getRecordcounter()-1);
                    if(t.getMinPerPage().contains(o)&& p.getIndexVector().size()==0){
                        Vector d= new Vector();
                        d= t.getMinPerPage();
                        d.removeElementAt(PageNumber);
                        t.setMinPerPage(d);
                        d=null;
                    }
                    if(t.getMinPerPage().contains(o)&& p.getIndexVector().size()>0){
                        Vector d= new Vector();
                        d= t.getMinPerPage();
                        t.getMinPerPage().removeElementAt(PageNumber);
                        d.insertElementAt(p.getIndexVector().get(0),PageNumber);
                        t.setMinPerPage(d);
                        d=null;
                    }
                    Vector x =t.getVectorofpages();
                    x.removeElementAt(PageNumber);
                    x.insertElementAt(p,PageNumber);
                    t.setVectorofpages(x);
                    x=null;
                }
                else{
                    throw new DBAppException();

                }

                //serializing tables
                serialize("src\\main\\resources\\data\\tables",tables);

                //serializing TableNames
                serialize("src\\main\\resources\\data\\tableNames",TableNames);
                //serializing page
                serialize("src\\main\\resources\\data\\Pages\\"+tableName+PageNumber+".ser",p);
                boolean flag =true;

                deleteFromIndex(tableName,h,del);
            }}
            else {
                Vector colnameVec = new  Vector ();
                Vector  colvalueVec= new Vector();
                Enumeration names;
                Object o = null;

                int PageNumber;
                int index;
                names=columnNameValue.keys();
                String stri;
                while(names.hasMoreElements()) {
                    stri=(String)names.nextElement();

                    colnameVec.addElement(stri);
                    colvalueVec.addElement(columnNameValue.get(stri));
                }
                int z = TableNames.indexOf(tableName);
                Table ta= tables.elementAt(z);
                Vector k= ta.getVectorofpages();
                for(int j=0;j<k.size();j++){
                    int rr=((Page) k.get(j)).getPageNum();

                    //deserializing page
                    Page pa = deserpage("src\\main\\resources\\data\\Pages\\"+tableName+rr+".ser");

                    Vector  s=pa.getPagevector();
                    for(int n=0; n<s.size();n++){
                        Vector colnamevlue = new Vector();

                        Hashtable <String,Object> row =(Hashtable<String, Object>) s.elementAt(n);

                        for(int y=0;y<colnameVec.size();y++){
                            colnamevlue.addElement(row.get(colnameVec.get(y)));

                            o=  row.get(t.getClusteringKey());
                        }
                        if(colvalueVec.equals(colnamevlue)){

                            PageNumber=pa.getPageNum();
                            index=n;
                            Vector f= ta.getRecordsintable();
                            int jk=f.indexOf(o);
                            f.removeElementAt(jk);
                            ta.setRecordsintable(f);
                            f=null;
                            Vector sa=  pa.getIndexVector();
                            //EDITTTT
                            del.add(pa.getIndexVector().get(index));
                            sa.removeElementAt(index);
                           // sa.setElementAt(null,index);
                            pa.setIndexVector(sa);
                            sa=null;
                            Vector re=  pa.getPagevector();
                            re.removeElementAt(index);
                            //re.setElementAt(null,index);
                            pa.setPagevector(re);
                            re=null;
                            ta.setRecordcounter(ta.getRecordcounter()-1);
                            if(t.getMinPerPage().contains(o)&& pa.getIndexVector().size()==0) {
                                Vector d = new Vector();
                                d = t.getMinPerPage();
                                t.getMinPerPage().removeElementAt(PageNumber);
                                t.setMinPerPage(d);
                                d=null;
                            }
                            if(t.getMinPerPage().contains(o)&& pa.getIndexVector().size()>1){
                                Vector d= new Vector();
                                d= t.getMinPerPage();
                                t.getMinPerPage().removeElementAt(PageNumber);
                                d.insertElementAt(pa.getIndexVector().get(0),PageNumber);
                                t.setMinPerPage(d);
                                d=null;
                            }

                            //serializing tables
                            serialize("src\\main\\resources\\data\\tables",tables);


                            //serializing page
                            serialize("src\\main\\resources\\data\\Pages\\"+tableName+PageNumber+".ser",pa);
                        }

                    }
                }
                deleteFromIndex(tableName,columnNameValue,del);
            }
        }
        else {
            throw new DBAppException();
        }



    }
    public void deleteFromIndex(String Tablename, Hashtable colNameValue,Vector IDs){
        TableNames = desertable("src\\main\\resources\\data\\tableNames");
        tables = desertable("src\\main\\resources\\data\\tables");
        int loc = TableNames.indexOf(Tablename);
        Table t = tables.get(loc);
        if(t.getIndicesInTable().size()!=0) {
            Vector<GridIndex> tmp = new Vector<GridIndex>();
            for (int i = 0; i < t.getIndicesInTable().size(); i++) {
                GridIndex g = t.getIndicesInTable().get(i);
                String[] columnNames = g.getColumnnames();
                int[] divsindexes = new int[columnNames.length];
                for (int j = 0; j < columnNames.length; j++) {
                    String colname = (String) columnNames[j];
                    int indexvalue = getIndexinDivVec(g, colname, colNameValue.get(colname));
                    divsindexes[j] = indexvalue;
                }
                int bucketnumber = concatenateindex(divsindexes, g.getVectorofColumnDivs());
                Bucket b = deserBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser");
                //Bucket b = (Bucket) g.getRefBucketArray().get(bucketnumber);
                if (b != null) {
                    for (int r = 0; r < b.getPageNames().size(); r++) {
                        for (int k = 0; k < b.getRowEntriesInPage().get(r).size(); k++) {
                            if (compareObjects(IDs.get(r), b.getRowEntriesInPage().get(r).get(k).getRowValues().get(t.getClusteringKey())) == 0) {
                                Vector<Vector<entryInBucket>> RowEntriesInPage =  b.getRowEntriesInPage();
                                RowEntriesInPage.get(r).removeElementAt(k);
                                b.setRowEntriesInPage(RowEntriesInPage);
                                RowEntriesInPage=null;
                            }
                        }
                        if (b.getRowEntriesInPage().get(r).size() == 0) {
                            Vector pn =  b.getPageNames();
                            pn.removeElementAt(r);
                            b.setPageNames(pn);
                            pn=null;
                        }
                    }
                }


                //generate divs and get indices to know the indices in the refBucketArray
                //then delete elements from buckets in these cells in the vector refbucketarray then go remove these elements from pages by page no. and primary keys
                serializeBucket("src\\main\\resources\\data\\Buckets\\" + g.getRefBucketArray().get(bucketnumber) + ".ser", b);
                serialize("src\\main\\resources\\data\\tables", tables);
            }
        }
        /*if(b.getPageNames().size()==0){
            Path path = FileSystems.getDefault().getPath("src\\main\\resources\\data\\Buckets\\"+  g.getRefBucketArray().get(bucketnumber) +".ser");
            try {
                Files.delete(path);
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
            throws DBAppException {
        TableNames = desertable("src\\main\\resources\\data\\tableNames");
        tables = desertable("src\\main\\resources\\data\\tables");

        Vector<Vector> results=new Vector<Vector>();
        Vector finalresult=new Vector();
        for (int i = 0; i < sqlTerms.length; i++) {
            if(!TableNames.contains(sqlTerms[i].getStrTableName()))
                throw new DBAppException();
            int loc=TableNames.indexOf(sqlTerms[i].getStrTableName());
            Table t =tables.get(loc);
            boolean hasindex=false;
            int gridindex = 0;
            boolean multindex=false;
            if (arrayOperators[0].equals("AND") && t.getIndicesInTable().size()>0) {
                String[] cols = new String[sqlTerms.length];
                Vector columnsNam = new Vector();

                for (int y = 0; y < sqlTerms.length; y++) // adding all entered columns in an Array of Strings
                {
                    cols[y] = sqlTerms[y].getStrColumnName(); // array list of entered columns
                }
                Collections.addAll(columnsNam,cols);
                for (int o = 0; o < t.getIndicesInTable().size(); o++)// looping on all gridIndexes in the GIVEN TABLE
                {
                    GridIndex g = t.getIndicesInTable().get(o);
                    String[] columnnames = g.getColumnnames();
                    if (containsallArray(columnnames,cols) && columnnames.length == cols.length)  // CHECKING IF ANY INDEX CONTAINS THE WHOLE STRING OF COLUMNS AND THAT THEY HAVE THE SAME SIZE
                    {
                        // Make the 6 CASES >,<,>=,<=,!=
                        String[] l = cols;
                        gridindex = o;
                        multindex = true;
                        break;
                    }
                }

                if (multindex == true)
                {
                    GridIndex g = t.getIndicesInTable().get(gridindex);
                    Vector refbucket = g.getRefBucketArray();
                    Vector sqlresult = new Vector();
                    int[] divValues = new int[cols.length];

                    for (int c = 0; c < cols.length; c++)
                    {
                        int singledivs = getIndexinDivVec(g, sqlTerms[c].getStrColumnName(), sqlTerms[c].getObjValue());
                        divValues[c] = singledivs;
                    }
                    Vector<pageswithrows> ee = new Vector<pageswithrows>();
                    switch (sqlTerms[0].getStrOperator()) {

                        case "=":
                            //concatenateindex(int[] indices,Vector <customizedvector> vec)
                            int concatenate = concatenateindex(divValues, g.getVectorofColumnDivs()); //index in REF-BUCKET

                            if (refbucket.get(concatenate) != null) {
                                Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(concatenate) + ".ser");

                                Vector rr = new Vector();
                                for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                {
                                    Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                    pageswithrows pw = new pageswithrows();
                                    pw.setPagename(b1.getPageNames().get(entries));
                                    for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                        entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                        Hashtable x = s.getRowValues();  //Hashtable
                                        boolean flag3 = true;
                                        for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                        {
                                            for(int jk =iii;jk<x.size();jk++)
                                            {
                                                if (columnsNam.contains(t.getClusteringKey()))
                                                {
                                                    if (!(compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) == 0))
                                                    {
                                                        flag3 = false;
                                                        break;
                                                    }
                                                }

                                                else
                                                {
                                                    if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                    {
                                                        flag3 = false;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        if (flag3 == true) {
                                            rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                            pw.setRownumbers(rr);
                                            if (!(ee.contains(pw))) {
                                                ee.add(pw);
                                            }
                                        }

                                    }

                                }

                            }
                            break;
                        case ">":
                            int concatenate2 = concatenateindex(divValues, g.getVectorofColumnDivs());

                            if (refbucket.get(concatenate2) != null) {
                                for (int l = concatenate2; l < g.getRefBucketArray().size(); l++) {
                                    if (g.getRefBucketArray().get(l) != null) {
                                        Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(l) + ".ser");


                                        for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                        {
                                            Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                            Vector rr = new Vector();
                                            pageswithrows pw = new pageswithrows();
                                            pw.setPagename(b1.getPageNames().get(entries));
                                            for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                                entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                                Hashtable x = s.getRowValues();  //Hashtable
                                                boolean flag3 = true;
                                                for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                                {
                                                    for(int jk =iii;jk<x.size();jk++)
                                                    {
                                                        if (columnsNam.contains(t.getClusteringKey()))
                                                        {
                                                            if (!(compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) > 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }

                                                        else
                                                        {
                                                            if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                if (flag3 == true) {
                                                    rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                                    pw.setRownumbers(rr);
                                                    if (!(ee.contains(pw))) {
                                                        ee.add(pw);
                                                    }
                                                }


                                            }

                                        }
                                    }
                                }
                            }
                            break;

                        case "<":

                            int concatenate3 = concatenateindex(divValues, g.getVectorofColumnDivs());
                            if (refbucket.get(concatenate3) != null) {
                                for (int l = 0; l < concatenate3; l++) {
                                    if (g.getRefBucketArray().get(l) != null) {
                                        Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(l) + ".ser");


                                        for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                        {
                                            Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                            Vector rr = new Vector();
                                            pageswithrows pw = new pageswithrows();
                                            pw.setPagename(b1.getPageNames().get(entries));
                                            for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                                entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                                Hashtable x = s.getRowValues();  //Hashtable
                                                boolean flag3 = true;
                                                for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                                {
                                                    for(int jk =iii;jk<x.size();jk++)
                                                    {
                                                        if (columnsNam.contains(t.getClusteringKey()))
                                                        {
                                                            if (!(compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) < 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }

                                                        else
                                                        {
                                                            if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                if (flag3 == true) {
                                                    rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                                    pw.setRownumbers(rr);
                                                    if (!(ee.contains(pw))) {
                                                        ee.add(pw);
                                                    }
                                                }
                                            }

                                        }
                                    }
                                }
                            }
                            break;


                        case ">=":

                            int concatenate4 = concatenateindex(divValues, g.getVectorofColumnDivs());
                            if (refbucket.get(concatenate4) != null) {
                                for (int l = concatenate4; l < g.getRefBucketArray().size(); l++) {
                                    if (g.getRefBucketArray().get(l) != null) {
                                        Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(l) + ".ser");


                                        for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                        {
                                            Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                            Vector rr = new Vector();
                                            pageswithrows pw = new pageswithrows();
                                            pw.setPagename(b1.getPageNames().get(entries));
                                            for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                                entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                                Hashtable x = s.getRowValues();  //Hashtable
                                                boolean flag3 = true;
                                                for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                                {
                                                    for(int jk =iii;jk<x.size();jk++)
                                                    {
                                                        if (columnsNam.contains(t.getClusteringKey()))
                                                        {
                                                            if (!(compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) >= 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }

                                                        else
                                                        {
                                                            if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }

                                                if (flag3 == true) {
                                                    rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                                    pw.setRownumbers(rr);
                                                    if (!(ee.contains(pw))) {
                                                        ee.add(pw);
                                                    }
                                                }
                                               /* boolean flag6 =false;
                                                for(int ll=0;ll<ee.size();ll++){
                                                    for(int kk=0;kk<rr.size();kk++) {
                                                        if (ee.get(ll).getRownumbers().contains(rr.get(kk))) {
                                                            flag6 = true;
                                                        }
                                                    }
                                                }*/





                                            }

                                        }
                                    }
                                }
                            }
                            break;

                        case "<=":

                            int concatenate6 = concatenateindex(divValues, g.getVectorofColumnDivs());
                            if (refbucket.get(concatenate6) != null) {
                                for (int l = 0; l <= concatenate6; l++) {
                                    if (g.getRefBucketArray().get(l) != null) {
                                        Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(l) + ".ser");


                                        for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                        {
                                            Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                            Vector rr = new Vector();
                                            pageswithrows pw = new pageswithrows();
                                            pw.setPagename(b1.getPageNames().get(entries));
                                            for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                                entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                                Hashtable x = s.getRowValues();  //Hashtable
                                                boolean flag3 = true;
                                                for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                                {
                                                    for(int jk =iii;jk<x.size();jk++)
                                                    {
                                                        if (columnsNam.contains(t.getClusteringKey()))
                                                        {
                                                            if (!(compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) <= 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }

                                                        else
                                                        {
                                                            if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                            {
                                                                flag3 = false;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                if (flag3 == true) {
                                                    rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                                    pw.setRownumbers(rr);
                                                    if (!(ee.contains(pw))) {
                                                        ee.add(pw);
                                                    }
                                                }

                                            }

                                        }
                                    }
                                }
                            }
                            break;

                        case "!=":
                            int div5 = getIndexinDivVec(g, sqlTerms[i].getStrColumnName(), sqlTerms[i].getObjValue());
                            int divs5[] = new int[1];
                            divs5[0] = div5;
                            Vector antiresult = new Vector();
                            int concatenatedindex5 = concatenateindex(divs5, g.getVectorofColumnDivs());

                            for (int l = 0; l < g.getRefBucketArray().size(); l++) {
                                if (g.getRefBucketArray().get(l) != null) {
                                    Bucket b1 = deserBucket("src\\main\\resources\\data\\Buckets\\" + (String) refbucket.get(l) + ".ser");


                                    for (int entries = 0; entries < b1.getRowEntriesInPage().size(); entries++) // tuples in page
                                    {
                                        Vector<entryInBucket> e = b1.getRowEntriesInPage().get(entries);
                                        Vector rr = new Vector();
                                        pageswithrows pw = new pageswithrows();
                                        pw.setPagename(b1.getPageNames().get(entries));
                                        for (int entries2 = 0; entries2 < b1.getRowEntriesInPage().get(entries).size(); entries2++) {
                                            entryInBucket s = b1.getRowEntriesInPage().get(entries).get(entries2);
                                            Hashtable x = s.getRowValues();  //Hashtable
                                            boolean flag3 = true;
                                            for (int iii = 0; iii < sqlTerms.length; iii++)// Object Values
                                            {
                                                for(int jk =iii;jk<x.size();jk++)
                                                {
                                                    if (columnsNam.contains(t.getClusteringKey()))
                                                    {
                                                        if ((compareObjects(x.get((sqlTerms[jk].getStrColumnName())), sqlTerms[jk].getObjValue()) == 0))
                                                        {
                                                            flag3 = false;
                                                            break;
                                                        }
                                                    }

                                                    else
                                                    {
                                                        if (!(x.get(sqlTerms[jk].getStrColumnName()).equals(t.getClusteringKey())) && !(compareObjects(x.get(sqlTerms[jk].getStrColumnName()), sqlTerms[jk].getObjValue()) == 0))
                                                        {
                                                            flag3 = false;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            if (flag3 == true) {
                                                rr.add(b1.getRowEntriesInPage().get(entries).get(entries2).getRowNum());
                                                pw.setRownumbers(rr);
                                                if (!(ee.contains(pw))) {
                                                    ee.add(pw);
                                                }
                                            }

                                        }

                                    }
                                }
                            }
                            break;
                    }
                    for(int kk=0;kk<ee.size();kk++){
                        Page p = deserpage("src\\main\\resources\\data\\Pages\\" + ee.get(kk).getPagename() + ".ser");
                        for(int jj=0;jj<ee.get(kk).getRownumbers().size();jj++){
                            sqlresult.add(p.getPagevector().get((Integer) ee.get(kk).getRownumbers().get(jj)));
                        }
                    }
                    return sqlresult.iterator();
                }
            }
            else{
            for(int j =0;j<t.getIndicesInTable().size();j++){
                GridIndex g= t.getIndicesInTable().get(j);
                String[] columnnames=g.getColumnnames();
                Vector colname=new Vector();
                colname.addAll(Arrays.asList(columnnames));
                if(colname.contains(sqlTerms[i].getStrColumnName())&& colname.size()==1){
                    hasindex=true;
                    gridindex=j;
                    break;
                }
            }
            if(hasindex)
            {//if the column has a 1D index
                GridIndex g=t.getIndicesInTable().get(gridindex);
                Vector refbucket=g.getRefBucketArray();
                Vector sqlresult=new Vector();
                switch(sqlTerms[i].getStrOperator()){
                    case "=":
                        int div = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs[]=new int[1];
                        divs[0]=div;
                        int concatenatedindex=concatenateindex(divs,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex)!=null){
                            Bucket b1=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(concatenatedindex)+".ser");
                            for(int p=0;p<b1.getPageNames().size();p++){
                                for(int entries=0;entries<b1.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b1.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b1.getPageNames().get(p)+".ser");
                                        Record r =new Record(t.getClusteringKey());
                                        r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                        sqlresult.add(r);
                                    }

                                }
                            }
                            if(b1.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b1.getOverFlowBuckets().size();j++){
                                    Bucket an=b1.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                sqlresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }

                        }
                        break;
                    case ">":
                        int div2 = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs2[]=new int[1];
                        divs2[0]=div2;
                        int concatenatedindex2=concatenateindex(divs2,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex2)!=null){
                            Bucket b12=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(concatenatedindex2)+".ser");
                            for(int p=0;p<b12.getPageNames().size();p++){
                                for(int entries=0;entries<b12.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b12.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b12.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                            }
                            if(b12.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b12.getOverFlowBuckets().size();j++){
                                    Bucket an=b12.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                sqlresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }
                        }

                        for(int bucketn=concatenatedindex2;bucketn<g.getRefBucketArray().size();bucketn++){
                            if(refbucket.get(bucketn)!=null){
                                Bucket a12=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(bucketn)+".ser");
                                for(int p=0;p<a12.getPageNames().size();p++){
                                    for(int entries=0;entries<a12.getRowEntriesInPage().get(p).size();entries++){
                                        entryInBucket e = a12.getRowEntriesInPage().get(p).get(entries);
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+a12.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                                if(a12.getOverFlowBuckets().size()>0){
                                    for(int j=0;j<a12.getOverFlowBuckets().size();j++){
                                        Bucket k = a12.getOverFlowBuckets().get(j);
                                        for(int p=0;p<k.getPageNames().size();p++){
                                            for(int entries=0;entries<k.getRowEntriesInPage().get(p).size();entries++){
                                                entryInBucket e = k.getRowEntriesInPage().get(p).get(entries);
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+k.getPageNames().get(p)+".ser");
                                                Record r = (Record) page.getPagevector().get(e.getRowNum());
                                                sqlresult.add(r);
                                            }
                                        }
                                    }
                                }

                            }
                        }
                        break;
                    case "<":
                        int div3 = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs3[]=new int[1];
                        divs3[0]=div3;
                        int concatenatedindex3=concatenateindex(divs3,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex3)!=null){
                            Bucket b13=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(concatenatedindex3)+".ser");
                            for(int p=0;p<b13.getPageNames().size();p++){
                                for(int entries=0;entries<b13.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b13.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b13.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                            }
                            if(b13.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b13.getOverFlowBuckets().size();j++){
                                    Bucket an=b13.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                sqlresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }
                        }
                        for(int bucketn=concatenatedindex3;bucketn>0;bucketn--){
                            if(refbucket.get(bucketn)!=null){
                                Bucket a13=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(bucketn)+".ser");
                                for(int p=0;p<a13.getPageNames().size();p++){
                                    for(int entries=0;entries<a13.getRowEntriesInPage().get(p).size();entries++){
                                        entryInBucket e = a13.getRowEntriesInPage().get(p).get(entries);
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+a13.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                                if(a13.getOverFlowBuckets().size()>0){
                                    for(int j=0;j<a13.getOverFlowBuckets().size();j++){
                                        Bucket k = a13.getOverFlowBuckets().get(j);
                                        for(int p=0;p<k.getPageNames().size();p++){
                                            for(int entries=0;entries<k.getRowEntriesInPage().get(p).size();entries++){
                                                entryInBucket e = k.getRowEntriesInPage().get(p).get(entries);
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+k.getPageNames().get(p)+".ser");
                                                Record r = (Record) page.getPagevector().get(e.getRowNum());
                                                sqlresult.add(r);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        break;
                    case ">=":
                        int div4 = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs4[]=new int[1];
                        divs4[0]=div4;
                        int concatenatedindex4=concatenateindex(divs4,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex4)!=null){
                            Bucket b14=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(concatenatedindex4)+".ser");
                            for(int p=0;p<b14.getPageNames().size();p++){
                                for(int entries=0;entries<b14.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b14.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>=0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b14.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                            }
                            if(b14.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b14.getOverFlowBuckets().size();j++){
                                    Bucket an=b14.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>=0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                sqlresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }
                        }
                        for(int bucketn=concatenatedindex4;bucketn<g.getRefBucketArray().size();bucketn++){
                            if(refbucket.get(bucketn)!=null){
                                Bucket a14=deserBucket("src\\main\\resources\\data\\Buckets\\"+refbucket.get(bucketn)+"ser");
                                for(int p=0;p<a14.getPageNames().size();p++){
                                    for(int entries=0;entries<a14.getRowEntriesInPage().get(p).size();entries++){
                                        entryInBucket e = a14.getRowEntriesInPage().get(p).get(entries);
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+a14.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                                if(a14.getOverFlowBuckets().size()>0){
                                    for(int j=0;j<a14.getOverFlowBuckets().size();j++){
                                        Bucket k = a14.getOverFlowBuckets().get(j);
                                        for(int p=0;p<k.getPageNames().size();p++){
                                            for(int entries=0;entries<k.getRowEntriesInPage().get(p).size();entries++){
                                                entryInBucket e = k.getRowEntriesInPage().get(p).get(entries);
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+k.getPageNames().get(p)+".ser");
                                                Record r = (Record) page.getPagevector().get(e.getRowNum());
                                                sqlresult.add(r);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case "<=":
                        int div6 = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs6[]=new int[1];
                        divs6[0]=div6;
                        int concatenatedindex6=concatenateindex(divs6,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex6)!=null){
                            Bucket b16=deserBucket("src\\main\\resources\\data\\Buckets\\"+ refbucket.get(concatenatedindex6)+".ser");
                            for(int p=0;p<b16.getPageNames().size();p++){
                                for(int entries=0;entries<b16.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b16.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<=0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b16.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                            }
                            if(b16.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b16.getOverFlowBuckets().size();j++){
                                    Bucket an=b16.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<=0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                sqlresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }
                        }
                        for(int bucketn=concatenatedindex6;bucketn>0;bucketn--){
                            if(refbucket.get(bucketn)!=null){
                                Bucket a16=deserBucket("src\\main\\resources\\data\\Buckets\\"+refbucket.get(bucketn)+".ser");
                                for(int p=0;p<a16.getPageNames().size();p++){
                                    for(int entries=0;entries<a16.getRowEntriesInPage().get(p).size();entries++){
                                        entryInBucket e = a16.getRowEntriesInPage().get(p).get(entries);
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+a16.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        sqlresult.add(r);
                                    }
                                }
                                if(a16.getOverFlowBuckets().size()>0){
                                    for(int j=0;j<a16.getOverFlowBuckets().size();j++){
                                        Bucket k = a16.getOverFlowBuckets().get(j);
                                        for(int p=0;p<k.getPageNames().size();p++){
                                            for(int entries=0;entries<k.getRowEntriesInPage().get(p).size();entries++){
                                                entryInBucket e = k.getRowEntriesInPage().get(p).get(entries);
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+k.getPageNames().get(p)+".ser");
                                                Record r = (Record) page.getPagevector().get(e.getRowNum());
                                                sqlresult.add(r);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        break;
                    case "!=":
                        int div5 = getIndexinDivVec(g,sqlTerms[i].getStrColumnName(),sqlTerms[i].getObjValue());
                        int divs5[]=new int[1];
                        divs5[0]=div5;
                        Vector antiresult=new Vector();
                        int concatenatedindex5=concatenateindex(divs5,g.getVectorofColumnDivs());
                        if(refbucket.get(concatenatedindex5)!=null){
                            Bucket b15=deserBucket("src\\main\\resources\\data\\Buckets\\"+(String) refbucket.get(concatenatedindex5)+".ser");
                            for(int p=0;p<b15.getPageNames().size();p++){
                                for(int entries=0;entries<b15.getRowEntriesInPage().get(p).size();entries++){
                                    entryInBucket e = b15.getRowEntriesInPage().get(p).get(entries);
                                    if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0){
                                        Page page =deserpage("src\\main\\resources\\data\\Pages\\"+b15.getPageNames().get(p)+".ser");
                                        Record r = (Record) page.getPagevector().get(e.getRowNum());
                                        antiresult.add(r);
                                    }
                                }
                            }
                            if(b15.getOverFlowBuckets().size()>0){
                                for(int j=0;j<b15.getOverFlowBuckets().size();j++){
                                    Bucket an=b15.getOverFlowBuckets().get(j);
                                    for(int p=0;p<an.getPageNames().size();p++){
                                        for(int entries=0;entries<an.getRowEntriesInPage().get(p).size();entries++){
                                            entryInBucket e = an.getRowEntriesInPage().get(p).get(entries);
                                            if(compareObjects(e.getRowValues().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0){
                                                Page page =deserpage("src\\main\\resources\\data\\Pages\\"+an.getPageNames().get(p)+".ser");
                                                Record r =new Record(t.getClusteringKey());
                                                r.setFullrecord((Hashtable<String, Object>) page.getPagevector().get(e.getRowNum()));
                                                antiresult.add(r);
                                            }

                                        }
                                    }
                                }
                            }
                            Vector copy= t.getRecordsintable();
                            copy.removeAll(antiresult);
                            sqlresult=copy;
                        }
                        break;
                }
                results.add(sqlresult);

            }
            else{//if the column doenst have an index linear search in pages 2 casesif column is primary binary if not linear
                if(!t.getClusteringKey().equals(sqlTerms[i].getStrColumnName()) || t.getPagescounter()==1)
                {
                    Vector sqlresult=new Vector();
                    for(int pnum=0;pnum<t.getVectorofpages().size();pnum++)//linear if column is not primary
                    {
                        Page p = deserpage("src/main/resources/data/Pages/"+t.getTableName()+pnum+".ser");
                        for(int recordcount=0;recordcount<p.getPagevector().size();recordcount++)
                        {
                            Record r = new Record(t.getClusteringKey());
                            Hashtable hash = (Hashtable) p.getPagevector().get(recordcount);
                            r.setFullrecord(hash);
                            switch(sqlTerms[i].getStrOperator()){
                                case "=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case ">":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "<":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case ">=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "<=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "!=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())!=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                            }

                        }
                    }
                    results.add(sqlresult);
                }
                else
                {
                    Vector sqlresult = new Vector();
                    Vector minvect = t.getMinPerPage();
                    if(minvect.size()==1){
                        Page p = deserpage("src/main/resources/data/Pages/"+t.getTableName()+0+".ser");
                        for(int recordcount=0;recordcount<p.getPagevector().size();recordcount++)
                        {
                            Record r = new Record(t.getClusteringKey());
                            r.setFullrecord((Hashtable<String, Object>) p.getPagevector().get(recordcount));
                            switch(sqlTerms[i].getStrOperator()){
                                case "=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case ">":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "<":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case ">=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "<=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                                case "!=":
                                    if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())!=0)
                                    {
                                        sqlresult.add(r);
                                    }
                                    break;
                            }
                        }
                    }
                   else{
                    for(int h = 0;h<minvect.size()-1;h++)
                    {
                        if(compareObjects(minvect.get(h),sqlTerms[i].getObjValue())<=0  && compareObjects(minvect.get(h+1),sqlTerms[i].getObjValue()) > 0)
                        {
                            Page p = deserpage("src/main/resources/data/Pages/"+t.getTableName()+h+".ser");
                            for(int recordcount=0;recordcount<p.getPagevector().size();recordcount++)
                            {
                                Record r = (Record) p.getPagevector().get(recordcount);
                                switch(sqlTerms[i].getStrOperator()){
                                    case "=":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())==0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                    case ">":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                    case "<":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                    case ">=":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())>=0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                    case "<=":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())<=0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                    case "!=":
                                        if(compareObjects(r.getFullrecord().get(sqlTerms[i].getStrColumnName()),sqlTerms[i].getObjValue())!=0)
                                        {
                                            sqlresult.add(r);
                                        }
                                        break;
                                }
                            }
                        }
                    }}
                    results.add(sqlresult);
                }
            }
        }}
        if(sqlTerms.length==1){
           return results.get(0).iterator();
        }
        for(int i =0;i< arrayOperators.length;i++){
            switch(arrayOperators[i]){
                case "OR":
                    if(results.get(0)!=null || results.get(1)!=null){
                    Vector orresult= orOperator(results.get(0),results.get(1));
                    results.removeElementAt(1);results.removeElementAt(0);
                    results.add(0,orresult);}
                    break;
                case "AND":
                    if(results.get(0)!=null || results.get(1)!=null){
                    Vector andresult= andOperator(results.get(0),results.get(1));
                    results.removeElementAt(1);
                    results.removeElementAt(0);
                    results.add(0,andresult);}
                    break;
                case "XOR":
                    if(results.get(0)!=null || results.get(1)!=null){
                    Vector xorresult= xorOperator(results.get(0),results.get(1));
                    results.removeElementAt(1);results.removeElementAt(0);
                    results.add(0,xorresult);}
                    break;
            }
            finalresult=results.get(0);

        }
        return finalresult.iterator();
    }
    // get from the entered record the attributes that are inside the gridindex as Objects
    // then use the method indexinDiv vector which takes the Object one at a time and insert them all in one array
    // then use concatenateindex method by entering the array created in the previous step
    public static Vector andOperator(Vector a,Vector b){
        Vector z = new Vector();
        for(int i =0;i<b.size();i++){
            if(a.contains(b.get(i)))
                z.add(z.get(i));
        }
        return z;
    }
    public static Vector orOperator(Vector a,Vector b){
        Vector z = new Vector();
        z.addAll(a);
        for(int i=0;i<b.size();i++)
        {
            if(!a.contains(b.get(i)))
            {
                z.add(b.get(i));
            }
        }
        return z;
    }
    public static Vector xorOperator(Vector a,Vector b){
        Vector z = new Vector();
        for(int i=0;i<b.size();i++)
        {
            if(!a.contains(b.get(i)))
            {
                z.add(b.get(i));
            }
        }
        for(int i=0;i<a.size();i++)
        {
            if(!b.contains(a.get(i)))
            {
                z.add(b.get(i));
            }
        }
        return z;
    }
    public boolean containsallArray(String [] outer, String [] inner) {
        Vector<String> x  = new Vector<String>();
        x.addAll(Arrays.asList(outer));
        for (int i = 0; i < outer.length; i++)
        {
            if (!(x.contains(inner[i])))
            {
                return false;
            }
        }

        return true;
    }

    // change of static here
    public int getIndexInPage(int PageNumber,Record r,String tableName) {
        Page p = null;
        if ((new File("src\\main\\resources\\data\\Pages\\" + tableName + PageNumber + ".ser")).exists()) {
            //deserializing page
            p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + PageNumber + ".ser");
        }
        int first = 0;
        int last = p.getIndexVector().size() - 1;
        int mid = (first + last) / 2;
        Vector arr = p.getIndexVector();
        Hashtable<String, Object> f = r.getFullrecord();
        String cluster = r.getClusteringkey();
        Object key = r.getFullrecord().get(r.getClusteringkey());
        if (arr.size() == 0) {
            arr.insertElementAt(f.get(cluster), 0);
            p.setIndexVector(arr);
            serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser",p);
            arr=null;
            return 0;
        }
        while (first <= last) {

             if (compareObjects(arr.get(last), key) < 0) {
                arr.insertElementAt(f.get(cluster), last+1);
                p.setIndexVector(arr);
                serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser", p);
                arr=null;
                return last + 1;
            }
            else if (compareObjects(arr.get(first), key) > 0) {
                arr.insertElementAt(f.get(cluster), 0);
                p.setIndexVector(arr);
                serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser", p);
                arr=null;
                return 0;
            }
            else if (compareObjects(arr.get(mid), key) > 0)
                last = mid - 1;
            else if (compareObjects(arr.get(mid), key) < 0) {
                first = mid + 1;
            } else if (mid == first && mid == last && compareObjects(arr.get(mid), key) > 0) {
                arr.insertElementAt(f.get(cluster), mid);
                p.setIndexVector(arr);
                serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser",p);
                arr=null;
                return mid;
            } else if (mid == first && mid == last && compareObjects(arr.get(mid), key) < 0) {
                arr.insertElementAt(f.get(cluster), mid+1);
                p.setIndexVector(arr);
                serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser",p);
                arr=null;
                return mid + 1;
            }
            mid = (first + last) / 2;
        }
      //  System.out.println("error in index in page");
         return -1;
    }
    /*public int getIndexInPage(int PageNumber,Record r,String tableName){
        Page p;
        if((new File("src\\main\\resources\\data\\Pages\\"+tableName+PageNumber+".ser")).exists()) {
            //deserializing page
            p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + PageNumber + ".ser");

            Vector tmp;
            tmp = p.getIndexVector();
            Hashtable<String, Object> f = r.getFullrecord();

            String cluster = r.getClusteringkey();


            tmp.add(f.get(cluster));
            sorting(tmp);

            p.setIndexVector(tmp);

            //serializing page
            serialize("src\\main\\resources\\data\\Pages\\" + p.getTableName() + PageNumber + ".ser",p);
            return p.getIndexVector().indexOf((r.getFullrecord()).get(r.getClusteringkey()));
            //  }
        }
        else {
            return 0;
        }
    }*/
    public static int compareObjects(Object o,Object o2 ){
        if(o == null && o2== null ){
            return 0;
        }
        if((o!=null)&&(o2!=null)&&o.getClass().getName().equals("java.lang.String")){
            return o.toString().compareTo(o2.toString());
        }
        else if((o!=null)&&(o2!=null)&&o.getClass().getName().equals("java.util.Date")){
            return ((Date)o).compareTo((Date)o2);
        }
        else if((o!=null)&&(o2!=null)&&o.getClass().getName().equals("java.lang.Integer")){
            return (Integer)o- (Integer)o2 ;
        }
        else if((o!=null)&&(o2!=null)&&o.getClass().getName().equals("java.lang.Double")){
            if((Double) o>(Double) o2)
                return 1;
            else if(((Double) o).equals((Double) o2))
                return 0;
            else return -1;
        }
        return 0 ;
    }



    public void insert(Table t,Record r,int PageNumber,int index ){



        Vector vn = t.getRecordsintable();
        /*boolean flag=true;
        for(int i=0;i<vn.size();i++){
            Object tmp= vn.elementAt(i);
            String g=tmp.getClass().getName();
            if(g.equals("java.lang.String")){

                String s=r.getFullrecord().get(r.getClusteringkey()).toString();
                if(tmp.toString().equals(r.getFullrecord().get(r.getClusteringkey()).toString())){
                    try {
                        flag=false;
                        throw new DBAppException();
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(g.equals("java.lang.Integer")) {


                if (tmp.toString().equals(Integer.toString((int) r.getFullrecord().get(r.getClusteringkey())))) {
                    try {
                        flag = false;
                        throw new DBAppException();
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (g.equals("java.lang.Date")) {
                Date date = (Date) tmp;

                if (date.equals(r.getFullrecord().get(r.getClusteringkey()))) {
                    flag = false;
                    try {
                        throw new DBAppException();
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }
                }


            }

            if(g.equals("java.lang.Double")){
                Double d=(Double) tmp;
                if(d.equals(r.getFullrecord().get(r.getClusteringkey()))){
                    flag=false;
                    try {
                        throw new DBAppException();
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }
                }
            }

        }*/

        if(PageNumber>=0 && index>=0){

            try
            {



                if(!(new File("src\\main\\resources\\data\\Pages\\"+t.getTableName()+PageNumber+".ser")).exists()){
                    Page p = null;
                    p=new Page (t.getTableName(),PageNumber);

                    Vector tmp ;
                    tmp=p.getIndexVector();
                    Hashtable<String,Object> f = r.getFullrecord();

                    String cluster=r.getClusteringkey();
                    tmp.insertElementAt(f.get(cluster),index);
                    sorting(tmp);

                    p.setIndexVector(tmp);
                    Vector v =p.getPagevector();
                    v.insertElementAt(r.getFullrecord(),index);
                    r.getFullrecord().get(r.getClusteringkey());
                    vn.insertElementAt(r.getFullrecord().get(r.getClusteringkey()),index);
                    t.setRecordsintable(vn);
                    p.setPagevector(v);
                    t.setRecordcounter(t.getRecordcounter()+1);
                    Vector x = t.getVectorofpages();
                    x.removeElementAt(PageNumber);
                    x.insertElementAt(p,PageNumber);
                    t.setVectorofpages(x);

                    v=null;
                    x=null;
                    //serializing page
                    serialize("src\\main\\resources\\data\\Pages\\"+t.getTableName()+PageNumber+".ser",p);

                }
                else{


                    FileInputStream file = new FileInputStream("src\\main\\resources\\data\\Pages\\"+t.getTableName()+PageNumber+".ser");
                    ObjectInputStream in = new ObjectInputStream(file);
                    Page p = (Page)in.readObject();
                    Vector v =p.getPagevector();
                    v.insertElementAt(r.getFullrecord(),index);
                    r.getFullrecord().get(r.getClusteringkey());
                    vn.add(r.getFullrecord().get(r.getClusteringkey()));
                    t.setRecordsintable(vn);
                    p.setPagevector(v);
                    v=null;
                    t.setRecordcounter(t.getRecordcounter()+1);
                    Vector x = t.getVectorofpages();
                    int y=-1;
                    for(int m=0;m<t.getVectorofpages().size();m++){
                        if(t.getVectorofpages().get(m).getPageNum()==PageNumber){
                             y = m;
                        }
                    }
                    x.remove(y);
                    x.insertElementAt(p,y);
                    t.setVectorofpages(x);
                    x=null;
                    in.close();
                    file.close();

                    //serializing page
                    serialize("src\\main\\resources\\data\\Pages\\"+t.getTableName()+PageNumber+".ser",p);
                }
            }

            catch(IOException ex)
            {
                ex.printStackTrace();
            }

            catch(ClassNotFoundException ex)
            {
                ex.printStackTrace();
            }

            //serializing tables
        }


    }
    /*public static Vector sorting(Vector v ){
        v.size()
        int mid = (first + last)/2;
        while( first <= last ){
            if ( arr[mid] < key ){
                first = mid + 1;
            }else if ( arr[mid] == key ){
                System.out.println("Element is found at index: " + mid);
                break;
            }else{
                last = mid - 1;
            }
            mid = (first + last)/2;
        }
        if ( first > last ){
            System.out.println("Element is not found!");
        }
        return ;
    }*/

    public static Vector sorting(Vector v ){

        if (v.get(0).getClass().getName().equals("java.lang.Integer")){
            Vector g=new Vector();
            for(int i=0;i<v.size();i++){
                g.add(v.elementAt(i).toString());
            }
            Collections.sort(g);
            return g;

        }
        else if (v.get(0).getClass().getName().equals("java.lang.Double")){
            Vector g=new Vector();
            for(int i=0;i<v.size();i++){
                g.add(v.elementAt(i).toString());
            }
            Collections.sort(g);
            return g;

        }
        else if (v.get(0).getClass().getName().equals("java.util.Date")){
            Vector g=new Vector();
            for(int i=0;i<v.size();i++){
                g.add(v.elementAt(i).toString());
            }
            Collections.sort(g);

            return g;

        }
        else {
            Vector <String> g = v;
            Collections.sort(g);

            return g;
        }



    }


    public void updateTable(String tableName, String clusteringKeyValue,
                            Hashtable<String, Object> columnNameValue) throws DBAppException {
        if(TableNames.contains(tableName)) {
            int i = TableNames.indexOf(tableName);
            Table t =(Table)tables.get(i);
            if(t.getRecordcounter()==0) {
                throw new DBAppException();}
            // check if the primary key in the hashtable throw an exception
            else {
                if(!columnNameValue.containsKey(t.getClusteringKey())) {
                 /*Enumeration<String> ke = t.getColNameType().keys();
                    Enumeration<String> a = columnNameValue.keys();
                    Vector arr= new Vector();
                    Vector arr1= new Vector();
                    while(ke.hasMoreElements()){
                        arr.add(ke.nextElement());
                    }
                    while(a.hasMoreElements()){
                        arr1.add(a.nextElement());
                    }
                    if (!arr.containsAll(arr1)){
                        throw new DBAppException();
                    }
                    String clusterKey=t.getClusteringKey();
                    Record tmpRecord =new Record(clusterKey);
                    Hashtable<String,Object> tmp=columnNameValue;
                    tmp.put(clusterKey, clusteringKeyValue);
                    tmpRecord.setFullrecord(tmp);
                    int pNum=getPageNumber(t,tmpRecord);
                    ObjectInputStream in;
                    Page p = new Page(tableName,pNum);
                    //deserializing page
                    p = deserpage("src\\main\\resources\\data\\Pages\\"+tableName+pNum+".ser");

                    Vector k =p.getPagevector();
                    if(ColTypeCheck(columnNameValue, tableName)) {

                        for(int j=0;j<k.size();j++) {
                            Hashtable<String,Object> tmpR=(Hashtable<String, Object>) k.get(j);
                            if(tmpR.get(clusterKey).equals(clusteringKeyValue)) {
                                Enumeration en=tmpR.keys();
                                while(en.hasMoreElements()) {
                                    String str=(String) en.nextElement();
                                    tmpR.replace(str, columnNameValue.get(str));
                                }
                                k.remove(j);
                                k.add(j,tmpR);
                                break;
                            }

                        }
                        p.setPagevector(k);
                        Vector k1=t.getVectorofpages();
                        k1.remove(pNum);
                        k1.add(pNum, p);
                        t.setVectorofpages(k1);

                        //serializing page
                        serialize("src\\main\\resources\\data\\Pages\\"+tableName+pNum+".ser",p);
                    }
                    else {
                        throw new DBAppException();
                    }*/
                    Enumeration<String> ke = t.getColNameType().keys();
                    Enumeration<String> a = columnNameValue.keys();
                    Vector arr = new Vector();
                    Vector arr1 = new Vector();
                    while (ke.hasMoreElements()) {
                        arr.add(ke.nextElement());
                    }
                    while (a.hasMoreElements()) {
                        arr1.add(a.nextElement());
                    }
                    if (!arr.containsAll(arr1)) {
                        throw new DBAppException();
                    }
                    String clusterKey = t.getClusteringKey();
                    Record tmpRecord = new Record(clusterKey);
                    Hashtable<String, Object> tmp = (Hashtable<String, Object>) columnNameValue.clone();
                    tmp.put(clusterKey, clusteringKeyValue);
                    tmpRecord.setFullrecord(tmp);
                    int pNum = getPageNumber(t, tmpRecord);
                    ObjectInputStream in;
                    Page p = new Page(tableName, pNum);
                    //deserializing page
                    p = deserpage("src\\main\\resources\\data\\Pages\\" + tableName + pNum + ".ser");
                    Vector k = p.getPagevector();
                    if (ColTypeCheck(columnNameValue, tableName)) {
                        for (int j = 0; j < k.size(); j++) {
                            Hashtable<String, Object> tmpR = (Hashtable<String, Object>) k.get(j);
                            if (tmpR.get(clusterKey).equals(clusteringKeyValue)) {
                                Enumeration en = tmpR.keys();
                                while (en.hasMoreElements()) {
                                    String str = (String) en.nextElement();
                                    tmpR.replace(str, columnNameValue.get(str));
                                }
                                k.remove(j);
                                k.add(j, tmpR);
                                Hashtable<String, Object> h = new Hashtable();
                                h.put(t.getClusteringKey(), clusteringKeyValue);
                                deleteFromTable(tableName, h);
                                insertIntoTable(tableName, tmpR);
                                break;
                            }
                        }
                        p.setPagevector(k);
                        Vector k1 = t.getVectorofpages();
                        k1.remove(pNum);
                        k1.add(pNum, p);
                        t.setVectorofpages(k1);
                        k1=null;
                        //serializing page
                        serialize("src\\main\\resources\\data\\Pages\\" + tableName + pNum + ".ser", p);
                    }
                }
                else {
                    /*Enumeration<String> ke = t.getColNameType().keys();
                    Enumeration<String> a = columnNameValue.keys();
                    Vector arr= new Vector();
                    Vector arr1= new Vector();
                    while(ke.hasMoreElements()){
                        arr.add(ke.nextElement());
                    }
                    while(a.hasMoreElements()){
                        arr1.add(a.nextElement());
                    }
                    if (!arr.containsAll(arr1)){
                        throw new DBAppException();
                    }
                    String clusterKey=t.getClusteringKey();
                    Record tmpRecord =new Record(clusterKey);
                    Hashtable<String,Object> tmp=columnNameValue;
                    tmp.put(clusterKey, clusteringKeyValue);
                    tmpRecord.setFullrecord(tmp);
                    int pNum=getPageNumber(t,tmpRecord);
                    ObjectInputStream in;
                    Page p = new Page(tableName,pNum);
                    //deserializing page
                    p = deserpage("src\\main\\resources\\data\\Pages\\"+tableName+pNum+".ser");
                    Vector k =p.getPagevector();
                    if(ColTypeCheck(columnNameValue, tableName)) {
                        for(int j=0;j<k.size();j++) {
                            Hashtable<String,Object> tmpR=(Hashtable<String, Object>) k.get(j);
                            if(tmpR.get(clusterKey).equals(clusteringKeyValue)) {
                                Enumeration en=tmpR.keys();
                                while(en.hasMoreElements()) {
                                    String str=(String) en.nextElement();
                                    tmpR.replace(str, columnNameValue.get(str));
                                }
                                k.remove(j);
                                k.add(j,tmpR);
                                Hashtable<String,Object> h=new Hashtable();
                                h.put(t.getClusteringKey(),clusteringKeyValue);
                                deleteFromTable(tableName,h);
                                insertIntoTable(tableName,tmpR);
                                break;
                            }}
                        p.setPagevector(k);
                        Vector k1=t.getVectorofpages();
                        k1.remove(pNum);
                        k1.add(pNum, p);
                        t.setVectorofpages(k1);
                        //serializing page
                        serialize("src\\main\\resources\\data\\Pages\\"+tableName+pNum+".ser",p);
                    }
                    else {
                        throw new DBAppException();
                    }*/
                    throw new DBAppException();

                }
            }
        }
    }
    public static void serialize (String path,Vector v){
        try {
            FileOutputStream file1;
            file1 = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file1);
            out.writeObject(v);

            out.close();
            file1.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    public static void serialize (String path,Page v) {
        try {
            FileOutputStream file1;
            file1 = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file1);
            out.writeObject(v);

            out.close();
            file1.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static Page deserpage(String path) {
        try {
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            Page p =(Page)in.readObject();
            file.close();
            in.close();
            return p;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
        public static Bucket deserBucket(String path){
            try {
                FileInputStream file = new FileInputStream(path);
                ObjectInputStream in = new ObjectInputStream(file);
                Bucket p =(Bucket)in.readObject();
                file.close();
                in.close();
                return p;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
    }
    public static void serializeBucket (String path,Bucket v) {
        try {
            FileOutputStream file1;
            file1 = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file1);
            out.writeObject(v);

            out.close();
            file1.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static Vector desertable(String path){
        try {
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            Vector a =(Vector)in.readObject();
            file.close();
            in.close();
            return a;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }

    }

    public static boolean ColTypeCheck(Hashtable<String,Object> colNameValue,String tableName) {
        try {
            File myObj = new File("src\\main\\resources\\metadata.csv");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String row = myReader.nextLine();
                String[] data = row.split(",");
                if(!data[0].equals(tableName))//csv checks
                    continue;
                if(colNameValue.containsKey(data[1]) && !colNameValue.get(data[1]).getClass().getName().equals(data[2])){
                    System.out.println(colNameValue.get(data[1]).getClass().getName());
                    System.out.println(data[2]);
                    throw new DBAppException();
                }
                if (colNameValue.containsKey(data[1]) && !((( colNameValue.get(data[1])).toString().compareTo(data[5]))>=0 ) &&!((colNameValue.get(data[1])).toString().compareTo(data[6])<=0 )){
                    throw new DBAppException();
                }
            }

            myReader.close();
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (DBAppException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static Vector Configure(){
        Vector<Integer> x = new Vector<>();
        File configFile = new File("src\\main\\resources\\DBApp.config");

        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);

            String MaximumRowsCountinPage = props.getProperty("MaximumRowsCountinPage");
            String MaximumKeysCountinIndexBucket = props.getProperty("MaximumKeysCountinIndexBucket");

            int MaxRowsInPage = Integer.parseInt(MaximumRowsCountinPage);
            int MaxKeysInBucket = Integer.parseInt(MaximumKeysCountinIndexBucket);

            x.insertElementAt(MaxRowsInPage,0);
            x.insertElementAt(MaxKeysInBucket, 1);

            reader.close();
            return x;
        } catch (FileNotFoundException ex) {
            return x;
        } catch (IOException ex) {
            return x;
        }

    }


    public static void tes(){
        Vector y =Configure();
        Page p = null;
        if(p.getPagevector().size()==(Integer) y.get(0)){
            int NewPageNum=p.getPageNum()+1;
            if((new File("src\\main\\resources\\data\\Pages\\"+NewPageNum+".ser")).exists()){
                FileInputStream file;
                try {
                    file = new FileInputStream("src\\main\\resources\\data\\Pages\\"+NewPageNum+".ser");
                    ObjectInputStream in = new ObjectInputStream(file);
                    Page NextPage = (Page)in.readObject();
                    if(NextPage.getPagevector().size()==(Integer) y.get(0)){

                    }
                    else {
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }

        }
        else{
        }
    }

    public static void main (String[] args) {

        DBApp db = new DBApp();
        db.init();
        Hashtable<String, String> colNameType = new Hashtable();
        Hashtable<String, String> colNameMin = new Hashtable();
        Hashtable<String, String> colNameMax = new Hashtable();
        colNameType.put("First Name", "java.lang.String");
        colNameType.put("Last Name", "java.lang.String");
        colNameType.put("GPA","java.lang.Double");
        colNameMin.put("First Name", "a" );
        colNameMin.put("Last Name", "a");
        colNameMin.put("GPA","0.0");
        colNameMax.put("First Name", "ZZZZZZZZ");
        colNameMax.put("Last Name", "ZZZZZZZZ");
        colNameMax.put("GPA","4.0");
        Hashtable<String, Object> colNameValue = new Hashtable();
        colNameValue.put("First Name", "Ahmed");
        colNameValue.put("Last Name", "Ahmed");
        colNameValue.put("GPA",3.0);
        Hashtable<String, Object> colNameValue1 = new Hashtable();
        colNameValue1.put("First Name", "Basel");
        colNameValue1.put("Last Name", "Basel");
        colNameValue1.put("GPA",4.0);
        Hashtable<String, Object> colNameValue2 = new Hashtable();
        colNameValue2.put("First Name", "Iermo");
        colNameValue2.put("Last Name", "Kiri");
        Hashtable<String, Object> colNameValue3 = new Hashtable();
        colNameValue3.put("First Name", "Ahmed");
        colNameValue3.put("Last Name", "Asser");
        Hashtable<String, Object> colNameValue4 = new Hashtable();
        colNameValue4.put("First Name", "Iayed");
        colNameValue4.put("Last Name", "Alaa");
        Hashtable<String, Object> colNameValue5 = new Hashtable();
        colNameValue5.put("First Name", "Ibrahim");
        Hashtable<String, Object> colNameValue6 = new Hashtable();
        colNameValue6.put("First Name", "Zalwa");
        colNameValue6.put("Last Name", "Moma");
        Hashtable<String, Object> colNameValue7 = new Hashtable();
        colNameValue7.put("First Name", "zony");
        colNameValue7.put("Last Name", "joe");
        Hashtable<String, Object> colNameValue8 = new Hashtable();
        colNameValue8.put("First Name", "zzzo");
        colNameValue8.put("Last Name", "mohamed");
        Hashtable<String, Object> colNameValue9 = new Hashtable();
        colNameValue9.put("First Name", "Sayed");
        colNameValue9.put("Last Name", "Alaa");
        //Hashtable <String,Object>colNameValue6=new Hashtable();
        Hashtable<String, Object> colNameValue10 = new Hashtable();
       colNameValue10.put("First Name", "Iermo");
       colNameValue10.put("Last Name", "salwa");

        //String[] g = new String[]{"Last Name"};
        String[] f = new String[]{"First Name","Last Name","GPA"};
       // System.out.println(compareObjects("","A"));
        // Bucket b = db.deserBucket("src/main/resources/data/Buckets/test1.ser");
        // Bucket b1 = db.deserBucket("src/main/resources/data/Buckets/test2.ser");
        //System.out.println(b.getRowEntriesInPage());
        //System.out.println(b1.getRowEntriesInPage());
        // try {
        // db.createIndex("test",g);
        //} catch (DBAppException e) {
        // e.printStackTrace();
        //}

        /*int[] x =new int[]{2,2,2};
        Vector v = new Vector<customizedvector>();
        customizedvector v1 = new customizedvector("ID",0,20);






        customizedvector v2 = new customizedvector("ID",0,8);



        customizedvector v3 = new customizedvector("ID",0,8);

        customizedvector v4 = new customizedvector("ID",0,8);


        v.add(v1);
        v.add(v2);
        v.add(v3);
        //v.add(v4);
        System.out.println( db.concatenateindex(x,v));*/
        //   Hashtable <String,String>colNameType1 =new Hashtable();
        // Hashtable <String,String>colNameMin1 =new Hashtable();
        //Hashtable <String,String>colNameMax1 =new Hashtable();
        //colNameType1.put("id","java.lang.Integer");
        //colNameType1.put("gpa","java.lang.Double");
        //colNameMin1.put("id","1");
        //colNameMin1.put("gpa","0.0");
        //colNameMax1.put("id","10");
        //colNameMax1.put("gpa","2.0");







        /*String[] col = new String[] {"First Name","Last Name"};
        GridIndex g = new GridIndex(col,"test");
        System.out.println(db.getIndexinDivVec(g,"First Name","Kemo"));*/

         try {
         //db.createTable("test", "First Name", colNameType, colNameMin, colNameMax);
              //db.createTable("test1", "id", colNameType1, colNameMin1, colNameMax1);
             //db.createIndex("test",f);
              //db.insertIntoTable("test", colNameValue); //Iermo  2
              //db.insertIntoTable("test", colNameValue1); //Mido
             //String table = "students";
            // String[] index = {"id"};
            // db.createIndex(table, index);
             SQLTerm [] sqlTerms=new SQLTerm[3];
             SQLTerm sqlterm1=new SQLTerm();
             sqlterm1.setStrTableName("test");
             sqlterm1.setObjValue("Basel");
             sqlterm1.setStrOperator("=");
             sqlterm1.setStrColumnName("First Name");
             SQLTerm sqlterm2=new SQLTerm();
             sqlterm2.setStrTableName("test");
             sqlterm2.setObjValue("Basel");
             sqlterm2.setStrOperator("=");
             sqlterm2.setStrColumnName("Last Name");
             SQLTerm sqlterm3=new SQLTerm();
             sqlterm3.setStrTableName("test");
             sqlterm3.setObjValue(3.0);
             sqlterm3.setStrColumnName("GPA");
             sqlterm3.setStrOperator("=");
             String [] operators=new String[]{"AND"};
             sqlTerms[0]=sqlterm1;
             sqlTerms[1]=sqlterm2;
             sqlTerms[2]=sqlterm3;
             Iterator value=db.selectFromTable(sqlTerms,operators);
             while (value.hasNext()) {
                 System.out.println(value.next());
             }
             //db.insertIntoTable("test", colNameValue3);  //Ahmed 1
             //db.insertIntoTable("test", colNameValue9);  //Sayed
             //db.insertIntoTable("test", colNameValue);   //Ibrahim
            // db.insertIntoTable("test", colNameValue4); //Iayed
             //db.insertIntoTable("test", colNameValue6);

             // db.deleteFromTable("test", colNameValue1);
             //db.deleteFromTable("test", colNameValue2);

            //db.updateTable("test", "Iermo", colNameValue10);

             //db.insert(t, r, 0, 1);
             //db.insert("test", r, 0, 0);
             // String[]strarrOperators = new String[1];
             //strarrOperators[0]="First Name";

             //db.createIndex("test",strarrOperators);
             //db.createIndex("test1",strarrOperators);
       } catch (DBAppException e2) {
         e2.printStackTrace();
   }
    }

}



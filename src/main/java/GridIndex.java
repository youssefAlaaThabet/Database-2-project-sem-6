import java.io.*;
import java.util.*;

public class GridIndex implements Serializable {

    int arraysize;
    private Vector <customizedvector> vectorofColumnDivs = new Vector<>() ;
    //[[id],[gpa]]
    //[(0,2),(2,4)]
    private Vector<Object> refBucketArray;
    private String [] columnnames;
    private String Tablename;
    private int nextBucket;
    private int gridNum=0;



    public int getGridNum() {
        return gridNum;
    }

    public void setGridNum(int bucketNum) {
        this.gridNum = bucketNum;
    }



    public GridIndex(String [] columnnames, String Tablename)
    {
        this.columnnames = columnnames;
        this.Tablename = Tablename;
        nextBucket=0;

        for(int i = 0;i<columnnames.length;i++)
        {

            customizedvector z = new customizedvector(columnnames[i],minofcolumn(columnnames[i],Tablename),maxofcolumn(columnnames[i],Tablename),typeOfCol(columnnames[i],Tablename));
            vectorofColumnDivs.insertElementAt(z,i);

        }
        int sum = 1;
        for(int j = 0;j<vectorofColumnDivs.size();j++)
        {
            sum = sum*(vectorofColumnDivs.get(j).getDivisionVector().size());
        }
        refBucketArray = new Vector<Object>(sum);
        Collection<Object> arr = Arrays.asList(new Object[sum]);
        refBucketArray.addAll(arr);

    }

    public static Object maxofcolumn(String columnNames, String tableName)
    {
        BufferedReader csvReader = null;
        try
        {
            csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
        } catch (FileNotFoundException e2)
        {
            e2.printStackTrace();
        }
        try {
                File myObj = new File("src\\main\\resources\\metadata.csv");
                Scanner myReader = new Scanner(myObj);
                String alldata="";
                while (myReader.hasNextLine())
                {
                    String row = myReader.nextLine();
                    String[] data = row.split(",");
                    if (!data[0].equals(tableName))
                    {
                        //csv checks
                        continue;
                    } else
                    {


                        if (data[1].equals(columnNames))
                        {
                            return data[6];
                        }
                    }
                }
        }
        catch (FileNotFoundException e)
            {
                e.printStackTrace();
                return null;
            }
        return null;
    }


    public static String typeOfCol(String columnNames, String tableName)
    {
        BufferedReader csvReader = null;
        try
        {
            csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
        } catch (FileNotFoundException e2)
        {
            e2.printStackTrace();
        }
        try {
            File myObj = new File("src\\main\\resources\\metadata.csv");
            Scanner myReader = new Scanner(myObj);
            String alldata="";
            while (myReader.hasNextLine())
            {
                String row = myReader.nextLine();
                String[] data = row.split(",");
                if (!data[0].equals(tableName))
                {
                    //csv checks
                    continue;
                } else
                {


                    if (data[1].equals(columnNames))
                    {
                        return data[2];
                    }
                }
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            return null;
        }
        return null;
    }


    public static Object minofcolumn(String columnNames, String tableName)
    {
        BufferedReader csvReader = null;
        try
        {
            csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
        } catch (FileNotFoundException e2)
        {
            e2.printStackTrace();
        }
        try {
            File myObj = new File("src\\main\\resources\\metadata.csv");
            Scanner myReader = new Scanner(myObj);
            String alldata="";
            while (myReader.hasNextLine())
            {
                String row = myReader.nextLine();
                String[] data = row.split(",");
                if (!data[0].equals(tableName))
                {
                    //csv checks
                    continue;
                } else
                {


                    if (data[1].equals(columnNames))
                    {
                        return data[5];
                    }
                }
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            return null;
        }
        return null;
    }
    public int getArraysize() {
        return arraysize;
    }

    public Vector<Object> getRefBucketArray() {
        return refBucketArray;
    }

    public String[] getColumnnames() {
        return columnnames;
    }

    public String getTablename() {
        return Tablename;
    }

    public Vector<customizedvector> getVectorofColumnDivs() {
        return vectorofColumnDivs;
    }

    public int getNextBucket() {
        return nextBucket;
    }
    public void setNextBucket(int nextBucket) {
        this.nextBucket = nextBucket;
    }
    public void setRefBucketArray(Vector<Object> refBucketArray) {
        this.refBucketArray = refBucketArray;
    }


}

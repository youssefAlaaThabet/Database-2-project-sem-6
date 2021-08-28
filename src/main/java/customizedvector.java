import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class customizedvector implements Serializable {

    private String Columnname;
    private Vector<minmaxstructure> DivisionVector;
    private Object min;
    private Object max;
    private String type;
    public String getColumnname() {
        return Columnname;
    }

    public void setColumnname(String columnname) {
        Columnname = columnname;
    }



    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    public Vector getDivisionVector() {
        return DivisionVector;
    }

    public void setDivisionVector(Vector divisionVector) {
        DivisionVector = divisionVector;
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public customizedvector(String Columnname, Object min, Object max, String type)
    {
        this.Columnname = Columnname;
        this.DivisionVector = new Vector();
        this.min = min;
        this.max = max;
        this.type=type;

        if(type.equals("java.lang.String"))
        {
            if(min.equals("AAAAAA") && max.equals("ZZZZZZ")){

            minmaxstructure a=new minmaxstructure("AAAAAA","DAAAAA");  //1
            minmaxstructure b=new minmaxstructure("DAAAAA","GAAAAA");  //2
            minmaxstructure c=new minmaxstructure("GAAAAA","JAAAAA");  //3
            minmaxstructure d=new minmaxstructure("JAAAAA","MAAAAA");  //4
            minmaxstructure e=new minmaxstructure("MAAAAA","OAAAAA");  //5
            minmaxstructure f=new minmaxstructure("OAAAAA","QAAAAA");  //6
            minmaxstructure g=new minmaxstructure("QAAAAA","TAAAAA");  //7
            minmaxstructure h=new minmaxstructure("TAAAAA","VAAAAA");  //8
            minmaxstructure i=new minmaxstructure("VAAAAA","YAAAAA");  //9
            minmaxstructure j=new minmaxstructure("YAAAAA","Zzzzzza"); //10
            minmaxstructure k=new minmaxstructure(null,null);          //11

            DivisionVector.insertElementAt(a,0);
            DivisionVector.insertElementAt(b,1);
            DivisionVector.insertElementAt(c,2);
            DivisionVector.insertElementAt(d,3);
            DivisionVector.insertElementAt(e,4);
            DivisionVector.insertElementAt(f,5);
            DivisionVector.insertElementAt(g,6);
            DivisionVector.insertElementAt(h,7);
            DivisionVector.insertElementAt(i,8);
            DivisionVector.insertElementAt(j,9);
            DivisionVector.insertElementAt(k,10);
        }
          else  if(min.equals("AAAAAA") && max.equals("zzzzzz")){
                minmaxstructure a=new minmaxstructure("AAAAAA","GAAAAA");  //1
                minmaxstructure b=new minmaxstructure("GAAAAA","MAAAAA");  //2
                minmaxstructure c=new minmaxstructure("MAAAAA","QAAAAA");  //3
                minmaxstructure d=new minmaxstructure("QAAAAA","VAAAAA");  //4
                minmaxstructure e=new minmaxstructure("VAAAAA","ZZZZZZ");  //5
                minmaxstructure f=new minmaxstructure("ZZZZZZ","fzzzzz");  //6
                minmaxstructure g=new minmaxstructure("fzzzzz","lzzzzz");  //7
                minmaxstructure h=new minmaxstructure("lzzzzz","rzzzzz");  //8
                minmaxstructure i=new minmaxstructure("rzzzzz","uzzzzz");  //9
                minmaxstructure j=new minmaxstructure("uzzzzz","zzzzzz"); //10
                minmaxstructure k=new minmaxstructure(null,null);          //11

                DivisionVector.insertElementAt(a,0);
                DivisionVector.insertElementAt(b,1);
                DivisionVector.insertElementAt(c,2);
                DivisionVector.insertElementAt(d,3);
                DivisionVector.insertElementAt(e,4);
                DivisionVector.insertElementAt(f,5);
                DivisionVector.insertElementAt(g,6);
                DivisionVector.insertElementAt(h,7);
                DivisionVector.insertElementAt(i,8);
                DivisionVector.insertElementAt(j,9);
                DivisionVector.insertElementAt(k,10);
            }
          else  if(min.equals("43-0000")&& max.equals("99-9999")){
                minmaxstructure a=new minmaxstructure("43-0000","48-0000");  //1
                minmaxstructure b=new minmaxstructure("48-0000","53-0000");  //2
                minmaxstructure c=new minmaxstructure("53-0000","58-0000");  //3
                minmaxstructure d=new minmaxstructure("58-0000","63-0000");  //4
                minmaxstructure e=new minmaxstructure("63-0000","68-0000");  //5
                minmaxstructure f=new minmaxstructure("68-0000","73-0000");  //6
                minmaxstructure g=new minmaxstructure("73-0000","79-0000");  //7
                minmaxstructure h=new minmaxstructure("79-0000","86-0000");  //8
                minmaxstructure i=new minmaxstructure("86-0000","93-0000");  //9
                minmaxstructure j=new minmaxstructure("93-0000","99-9999"); //10
                minmaxstructure k=new minmaxstructure(null,null);          //11

                DivisionVector.insertElementAt(a,0);
                DivisionVector.insertElementAt(b,1);
                DivisionVector.insertElementAt(c,2);
                DivisionVector.insertElementAt(d,3);
                DivisionVector.insertElementAt(e,4);
                DivisionVector.insertElementAt(f,5);
                DivisionVector.insertElementAt(g,6);
                DivisionVector.insertElementAt(h,7);
                DivisionVector.insertElementAt(i,8);
                DivisionVector.insertElementAt(j,9);
                DivisionVector.insertElementAt(k,10);
            }
          else  if(min.equals("0000") && max.equals("9999")){
                minmaxstructure a=new minmaxstructure("0000","1000");  //1
                minmaxstructure b=new minmaxstructure("1000","2000");  //2
                minmaxstructure c=new minmaxstructure("2000","3000");  //3
                minmaxstructure d=new minmaxstructure("3000","4000");  //4
                minmaxstructure e=new minmaxstructure("4000","5000");  //5
                minmaxstructure f=new minmaxstructure("5000","6000");  //6
                minmaxstructure g=new minmaxstructure("6000","7000");  //7
                minmaxstructure h=new minmaxstructure("7000","8000");  //8
                minmaxstructure i=new minmaxstructure("8000","9000");  //9
                minmaxstructure j=new minmaxstructure("9000","10000"); //10
                minmaxstructure k=new minmaxstructure(null,null);          //11

                DivisionVector.insertElementAt(a,0);
                DivisionVector.insertElementAt(b,1);
                DivisionVector.insertElementAt(c,2);
                DivisionVector.insertElementAt(d,3);
                DivisionVector.insertElementAt(e,4);
                DivisionVector.insertElementAt(f,5);
                DivisionVector.insertElementAt(g,6);
                DivisionVector.insertElementAt(h,7);
                DivisionVector.insertElementAt(i,8);
                DivisionVector.insertElementAt(j,9);
                DivisionVector.insertElementAt(k,10);
            }
          else{

                    minmaxstructure a=new minmaxstructure("AAAAAA","DAAAAA");  //1
                    minmaxstructure b=new minmaxstructure("DAAAAA","GAAAAA");  //2
                    minmaxstructure c=new minmaxstructure("GAAAAA","JAAAAA");  //3
                    minmaxstructure d=new minmaxstructure("JAAAAA","MAAAAA");  //4
                    minmaxstructure e=new minmaxstructure("MAAAAA","OAAAAA");  //5
                    minmaxstructure f=new minmaxstructure("OAAAAA","QAAAAA");  //6
                    minmaxstructure g=new minmaxstructure("QAAAAA","TAAAAA");  //7
                    minmaxstructure h=new minmaxstructure("TAAAAA","VAAAAA");  //8
                    minmaxstructure i=new minmaxstructure("VAAAAA","YAAAAA");  //9
                    minmaxstructure j=new minmaxstructure("YAAAAA","Zzzzzza"); //10
                    minmaxstructure k=new minmaxstructure(null,null);          //11

                DivisionVector.insertElementAt(a,0);
                DivisionVector.insertElementAt(b,1);
                DivisionVector.insertElementAt(c,2);
                DivisionVector.insertElementAt(d,3);
                DivisionVector.insertElementAt(e,4);
                DivisionVector.insertElementAt(f,5);
                DivisionVector.insertElementAt(g,6);
                DivisionVector.insertElementAt(h,7);
                DivisionVector.insertElementAt(i,8);
                DivisionVector.insertElementAt(j,9);
                DivisionVector.insertElementAt(k,10);
            }
        }

        else
        {
            if (type.equals("java.util.Date"))
            {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                Date maxi = null;
                Date mini = null;
                try {
                    maxi = format.parse((String) max);
                    mini = format.parse((String) min);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                long diffInMili = Math.abs((maxi.getTime())-mini.getTime());
                long totDays = TimeUnit.DAYS.convert(diffInMili, TimeUnit.MILLISECONDS);
                long Divs = totDays/10;
                Calendar cal = Calendar.getInstance();
                cal.setTime(mini);
                for(int i=0;i<10;i++){
                    Calendar cal2 = (Calendar) cal.clone();
                    cal2.add(Calendar.DAY_OF_MONTH, (int) Divs);
                    minmaxstructure a=new minmaxstructure(cal.getTime(),cal2.getTime());
                    cal2.add(Calendar.DAY_OF_MONTH, (int) Divs);
                    cal.add(Calendar.DAY_OF_MONTH, (int) Divs);
                    DivisionVector.insertElementAt(a,i);
                }
                minmaxstructure k=new minmaxstructure(null,null);
                DivisionVector.insertElementAt(k,10);


            }
            else
            {
                if(type.equals("java.lang.Integer"))
                {
                    int mini = Integer.parseInt((String) min);
                    int maxi = Integer.parseInt((String) max);
                    int Diff = maxi - mini;
                    int tmp = mini;
                    if(Diff<10){
                        for (int i=0;i<Diff;i++)
                        {
                            if(i==Diff-1){
                                DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + 1+1), i);
                                DivisionVector.insertElementAt(new minmaxstructure(null,null),i+1);
                            }else {
                                DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + 1), i);
                                tmp = tmp + 1 ;
                            }


                        }
                    }
                    else {
                        int Divs = Diff / 10;
                        mini = Integer.parseInt((String) min);
                        tmp = mini;

                        for (int i = 0; i < 10; i++) {
                            if(i==Diff-1){
                                DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + Divs+Divs), i);
                                DivisionVector.insertElementAt(new minmaxstructure(null,null),i+1);
                            }else {
                                DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + Divs), i);
                                tmp = tmp + Divs ;
                            }
                        }
                    }
                }
                if(type.equals("java.lang.Double"))
                {
                    Double mini = Double.parseDouble((String) min);
                    Double maxi = Double.parseDouble((String) max);
                    double Diff = maxi - mini;
                    double tmp = mini;

                    /*if(Diff<10){
                        for (int i=0;i<Diff;i++)
                        {
                            DivisionVector.insertElementAt(new minmaxstructure(tmp,tmp+divdiff),i);
                            tmp = tmp + divdiff;

                        }
                    }*/
                    double Divs = Diff/10;
                    tmp = mini;

                    for (int i=0;i<10;i++)
                    {
                        if(i==9){
                            DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + Divs+Divs), i);
                            DivisionVector.insertElementAt(new minmaxstructure(null,null),i+1);
                        }else {
                            DivisionVector.insertElementAt(new minmaxstructure(tmp, tmp + Divs), i);
                            tmp = tmp + Divs ;
                        }

                    }

                }
            }
        }
    }
    public static void main(String[] args)
    {




    }
}

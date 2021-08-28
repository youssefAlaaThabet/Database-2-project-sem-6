import java.util.Vector;

public class test {
    public static void main(String[] args) {
        Vector v= new Vector();
        v.add(1);
        v.add(2);
        v.setElementAt(null,0);
        System.out.println(v.get(1));
    }
}

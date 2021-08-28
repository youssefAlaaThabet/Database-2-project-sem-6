import java.util.Collections;
import java.util.Date;
import java.util.Vector;

public class SQLTerm {
  String _strTableName;
 String _strColumnName;
 String _strOperator;
 Object _objValue;
    public String getStrTableName() {
        return _strTableName;
    }

    public void setStrTableName(String strTableName) {
        this._strTableName = strTableName;
    }

    public String getStrColumnName() {
        return _strColumnName;
    }

    public void setStrColumnName(String strColumnName) {
        this._strColumnName = strColumnName;
    }

    public String getStrOperator() {
        return _strOperator;
    }

    public void setStrOperator(String strOperator) {
        this._strOperator = strOperator;
    }

    public Object getObjValue() {
        return _objValue;
    }

    public void setObjValue(Object objValue) {
        this._objValue = objValue;
    }

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue){
      this._strTableName=strTableName;
      this._strColumnName=strColumnName;
      this._strOperator=strOperator;
      this._objValue=objValue;
    }
    public SQLTerm(){

    }
    public static void main(String[] args) {
        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        System.out.println(date_passed.toString());
    }
}

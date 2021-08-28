import java.io.Serializable;

public class minmaxstructure implements Serializable {



    private Object min;
    private Object max;

    public minmaxstructure(Object min , Object max)
    {
        this.min = min;
        this.max = max;
    }
    public Object getMin() {
        return min;
    }

    public Object getMax() {
        return max;
    }
}

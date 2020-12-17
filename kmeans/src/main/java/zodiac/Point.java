package zodiac;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements WritableComparable<Center> {

    private List<DoubleWritable> values;

    Point(List<DoubleWritable> values) {
        this.values = new ArrayList<DoubleWritable>();
        for (DoubleWritable p : values) {
            this.values.add(p);
        }
    }

    Point() {
        this.values = new ArrayList<DoubleWritable>();
    }

    Point(int numDimensions) {
        this.values = new ArrayList<DoubleWritable>();
        for (int i = 0; i < numDimensions; i++)
            values.add(new DoubleWritable(0.0));
    }

    public void readFields(DataInput dataInput) throws IOException {
        int iParams = dataInput.readInt();
        values = new ArrayList<DoubleWritable>();
        for (int i = 0; i < iParams; i++) {
            values.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(values.size());
        for (DoubleWritable p : values) {
            dataOutput.writeDouble(p.get());
        }
    }

    public String toString() {
        String elements = "";
        for (DoubleWritable e : values) {
            elements += e.get() + ";";
        }
        return elements;
    }

    public int compareTo(@Nonnull Center p) {
        return 0;
    }


    List<DoubleWritable> getValues() {
        return values;
    }

}

package zodiac;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;


public class Point implements WritableComparable<Center> {

    private List<DoubleWritable> values;

    Point() {
        values = new ArrayList<DoubleWritable>();
    }

    Point(int n) {
        values = new ArrayList<DoubleWritable>();
        for (int i = 0; i < n; i++)
            values.add(new DoubleWritable(0.0));
    }

    Point(List<DoubleWritable> values) {
        this.values = new ArrayList<DoubleWritable>();
        for (DoubleWritable p : values) {
            this.values.add(p);
        }
    }

    List<DoubleWritable> getValues() {
        return values;
    }

    public String toString() {
        String elements = "";
        for (DoubleWritable e : values) {
            elements += e.get() + ";";
        }
        return elements;
    }

    public void readFields(DataInput dataInput) throws IOException {
        int parameters = dataInput.readInt();
        values = new ArrayList<DoubleWritable>();
        for (int i = 0; i < parameters; i++) {
            values.add(new DoubleWritable(dataInput.readDouble()));
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(values.size());
        for (DoubleWritable p : values) {
            dataOutput.writeDouble(p.get());
        }
    }

    public int compareTo(@Nonnull Center p) {
        return 0;
    }

}

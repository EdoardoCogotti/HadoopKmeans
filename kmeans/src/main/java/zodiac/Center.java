package zodiac;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.util.List;
import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Center extends Point {

    // cluster index and
    // number of istances (Cardinality) of this center
    // We access them only by setter and getter
    private IntWritable centerIndex;
    private IntWritable centerCardinality;

    Center(List<DoubleWritable> list, IntWritable centerIndex, IntWritable centerCardinality) {
        super(list);
        this.centerIndex = new IntWritable(centerIndex.get());
        this.centerCardinality = new IntWritable(centerCardinality.get());
    }

    Center() {
        super();
    }

    Center(int numDimensions) {
        super(numDimensions);
        setCenterCardinality(new IntWritable(0));
    }

    Center(List<DoubleWritable> l) {
        super(l);
        centerIndex = new IntWritable(0);
        centerCardinality = new IntWritable(0);
    }

    Center(Center c) {
        super(c.getValues());
        setCenterCardinality(c.getCenterCardinality());
        setCenterIndex(c.getCenterIndex());
    }

    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        centerIndex = new IntWritable(dataInput.readInt());
        centerCardinality = new IntWritable(dataInput.readInt());
    }

    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(centerIndex.get());
        dataOutput.writeInt(centerCardinality.get());
    }

    @Override
    public int compareTo(@Nonnull Center c) {
        if (this.getCenterIndex().get() == c.getCenterIndex().get()) {
            return 0;
        }
        return 1;
    }

    boolean isConverged(Center c, Double threshold) {
        //get distance
        List<DoubleWritable> centList = this.getValues();
        List<DoubleWritable> qList = c.getValues();
        Double dist = 0.0;
        for (int i = 0; i < this.getValues().size(); i++) {
            dist += Math.pow(centList.get(i).get() - qList.get(i).get(), 2);
        }
        dist =  Math.sqrt(dist);

        return threshold > dist;
        //return threshold > Distance.findDistance(this, c);
    }

    public String toString() {
        return this.getCenterIndex() + ";" + super.toString();
    }

    void divideCoordinates() {
        for (int i = 0; i < this.getValues().size(); i++) {
            this.getValues().set(i, new DoubleWritable(this.getValues().get(i).get() /centerCardinality.get()));
        }
    }

    void addPoints(IntWritable i) {
        this.centerCardinality = new IntWritable(this.centerCardinality.get() + i.get());
    }

    IntWritable getCenterIndex() {
        return centerIndex;
    }

    IntWritable getCenterCardinality() {
        return centerCardinality;
    }

    void setCenterIndex(IntWritable centerIndex) {
        this.centerIndex = new IntWritable(centerIndex.get());
    }

    void setCenterCardinality(IntWritable centerCardinality) {
        this.centerCardinality = new IntWritable(centerCardinality.get());
    }
}

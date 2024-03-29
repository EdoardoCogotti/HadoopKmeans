package zodiac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmeansCombiner extends Reducer<Center, Point, Center, Point> {

    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Point sumValues = new Point(conf.getInt("numDimensions", 2));
        int countValues = 0;
        Double temp;
        for (Point p : values) {
            for (int i = 0; i < p.getValues().size(); i++) {
                temp = sumValues.getValues().get(i).get() + p.getValues().get(i).get();
                sumValues.getValues().get(i).set(temp);
            }
            countValues++;
        }
        key.setCenterCardinality(new IntWritable(countValues));
        context.write(key, sumValues);
    }
}

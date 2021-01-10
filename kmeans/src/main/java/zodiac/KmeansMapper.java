package zodiac;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.io.IOException;

public class KmeansMapper extends Mapper<Object, Text, Center, Point> {
    private List<Center> centers = new ArrayList<Center>();

    protected void setup(Context context) throws IOException, InterruptedException {
        //get centroid file from the configurations 
        Path centersFilePath = new Path(context.getConfiguration().get("centersFilePath"));
        SequenceFile.Reader centersFileReader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(centersFilePath));

        IntWritable key = new IntWritable();
        Center value = new Center();
        //scroll all initial centroid in the file
        while (centersFileReader.next(key, value)) {
            //read center from file 
            Center c = new Center(value.getValues());
            c.setCenterCardinality(new IntWritable(0));
            c.setCenterIndex(key);

            //save file locally
            centers.add(c);
        }
        centersFileReader.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String pointRow = value.toString();
        List<DoubleWritable> pointValues = new ArrayList<DoubleWritable>();
        StringTokenizer tokenizer = new StringTokenizer(pointRow, ",");
        while (tokenizer.hasMoreTokens()) {
            pointValues.add(new DoubleWritable(Double.parseDouble(tokenizer.nextToken())));
        }
        Point queryPoint = new Point(pointValues);

        Center minDistanceCenter = null;
        Double minDistance = Double.MAX_VALUE;

        for (Center cent : centers) {
            List<DoubleWritable> centList = cent.getValues();
            List<DoubleWritable> qList = queryPoint.getValues();
            Double dist = 0.0;

            for (int i = 0; i < cent.getValues().size(); i++) {
                dist += Math.pow(centList.get(i).get() - qList.get(i).get(), 2);
            }
            dist =  Math.sqrt(dist);

            if (minDistance > dist) {
                minDistanceCenter = cent;
                minDistance = dist;
            }
        }
        context.write(minDistanceCenter, queryPoint);
    }


}

package zodiac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.IOException;

public class Map extends Mapper<Object, Text, Center, Point> {
    private List<Center> centers = new ArrayList<Center>();

    
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(conf.get("centersFilePath"))));

        IntWritable key = new IntWritable();
        Center value = new Center();
        
        //Cicle throw the centers in the file and save in local variable
        while (reader.next(key, value)) {
            Center c = new Center(value.getValues());

            c.setCenterIndex(key);
            c.setCenterCardinality(new IntWritable(0));

            centers.add(c);
        }

        reader.close();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<DoubleWritable> spaceValues = new ArrayList<DoubleWritable>();
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        while (tokenizer.hasMoreTokens()) {
            spaceValues.add(new DoubleWritable(Double.parseDouble(tokenizer.nextToken())));
        }
        Point queryPoint = new Point(spaceValues);

        Center minDistanceCenter = null;
        Double minDistance = Double.MAX_VALUE;
        Double distanceTemp=0.0;
        for (Center cent : centers) {
            //distanceTemp = Distance.findDistance(c, p);
            List<DoubleWritable> centList = cent.getValues();
            List<DoubleWritable> qList = queryPoint.getValues();
            Double dist = 0.0;
            for (int i = 0; i < cent.getValues().size(); i++) {
                dist += Math.pow(centList.get(i).get() - qList.get(i).get(), 2);
            }
            dist =  Math.sqrt(dist);

            //if (minDistance > distanceTemp) {
            if(minDistance > dist){
                minDistanceCenter = cent;
                minDistance = dist;
            }
        }
        context.write(minDistanceCenter, queryPoint);
    }


}

package zodiac;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;

public class KmeansReducer extends Reducer<Center, Point, IntWritable, Center> {

    private HashMap<IntWritable, Center> newCentersMap = new HashMap<IntWritable, Center>();
    private HashMap<IntWritable, Center> oldCentersMap = new HashMap<IntWritable, Center>();
    private int convCenters = 0;

    public enum CONVERGE_STATUS {
        CONVERGED
    }

    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        Center newCenter = new Center(configuration.getInt("numDimensions", 2));
        boolean flagOld = false;
        if (newCentersMap.containsKey(key.getCenterIndex())) {
            newCenter = newCentersMap.get(key.getCenterIndex());
            flagOld = true;
        }

        int newElements = 0;
        Double tmp;
        for (Point p : values) {
            for (int i = 0; i < p.getValues().size(); i++) {
                tmp = newCenter.getValues().get(i).get() + p.getValues().get(i).get();
                newCenter.getValues().get(i).set(tmp);
            }
            newElements += key.getCenterCardinality().get();
        }
        newCenter.setCenterIndex(key.getCenterIndex());
        newCenter.addPoints(new IntWritable(newElements));

        if (!flagOld) {
            newCentersMap.put(newCenter.getCenterIndex(), newCenter);
            oldCentersMap.put(key.getCenterIndex(), new Center(key));
        }

        context.write(newCenter.getCenterIndex(), newCenter);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        Path centersPath = new Path(configuration.get("centersFilePath"));
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(centersPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Iterator<Center> it = newCentersMap.values().iterator();
        Center newCenterValue;
        Center sameIndexC;
        Double avgValue = 0.0;
        Double conv_threshold = configuration.getDouble("conv_threshold", 0.5);
        int k = configuration.getInt("k", 2);
        while (it.hasNext()) {
            newCenterValue = it.next();
            newCenterValue.divideCoordinates();
            sameIndexC = oldCentersMap.get(newCenterValue.getCenterIndex());
            if (newCenterValue.isConverged(sameIndexC, conv_threshold))
                convCenters++;

            //get distance
            List<DoubleWritable> centList = newCenterValue.getValues();
            List<DoubleWritable> qList = sameIndexC.getValues();
            Double dist = 0.0;
            for (int i = 0; i < newCenterValue.getValues().size(); i++) {
                dist += Math.pow(centList.get(i).get() - qList.get(i).get(), 2);
            }
            dist =  Math.sqrt(dist);
            avgValue += Math.pow(dist,2);

            centerWriter.append(newCenterValue.getCenterIndex(), newCenterValue);
        }
        avgValue = Math.sqrt(avgValue / k);
        int percentSize = (newCentersMap.size() * 90) / 100;
        if (convCenters >= percentSize || avgValue < conv_threshold)
            context.getCounter(CONVERGE_STATUS.CONVERGED).increment(1);
        centerWriter.close();
    }

}

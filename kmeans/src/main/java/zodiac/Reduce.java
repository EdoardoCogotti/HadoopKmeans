package zodiac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;

public class Reduce extends Reducer<Center, Point, IntWritable, Center> {

    // We use hash maps in order to link cluster index (int) and cluster centers
    // oldCentersMap contains not updated values
    // newCentersMap contains sum values and cardinality of the cluster 
    // (new centers are evaluated in clean up phase)
    private HashMap<IntWritable, Center> newCentersMap = new HashMap<IntWritable, Center>();
    private HashMap<IntWritable, Center> oldCentersMap = new HashMap<IntWritable, Center>();
    private int convCenters = 0;

    public enum CONVERGE_STATUS { CONVERGED }

    @Override
    public void reduce(Center key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        // create a new center (Default dimension is 2)
        Center newCenter = new Center(configuration.getInt("numDimension", 2));

        // check if a new center for this cluster has already been calculated
        boolean alreadyInMapFlag = false;
        if (newCentersMap.containsKey(key.getCenterIndex())) {
            newCenter = newCentersMap.get(key.getCenterIndex());
            alreadyInMapFlag = true;
        }

        int newElements = 0;
        Double tempSum=0.0;
        for (Point p : values) {
            for (int i = 0; i < p.getValues().size(); i++) {
                //summing up all points of this cluster and
                // and temporarily update center values
                // getValues() in point class, get(i) from ArrayList , get() from Writable
                tempSum = newCenter.getValues().get(i).get() + p.getValues().get(i).get();
                newCenter.getValues().get(i).set(tempSum);
            }
            newElements += key.getCenterCardinality().get(); //to "know" size of an Iterable
        }
        newCenter.setCenterIndex(key.getCenterIndex());
        newCenter.addPoints(new IntWritable(newElements));

        if (!alreadyInMapFlag) {
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
        Double threshold = configuration.getDouble("conv_threshold", 0.5);
        int k = configuration.getInt("k", 2);
        while (it.hasNext()) {
            newCenterValue = it.next();
            newCenterValue.divideCoordinates();
            sameIndexC = oldCentersMap.get(newCenterValue.getCenterIndex());
            if (newCenterValue.isConverged(sameIndexC, threshold))
                convCenters++;

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
        if (convCenters >= percentSize || avgValue < threshold)
            context.getCounter(CONVERGE_STATUS.CONVERGED).increment(1);
        centerWriter.close();
    }

}

package zodiac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Kmeans {
    static public long startJob;
    static public long endJob;

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        startJob = System.currentTimeMillis();
        
        for(int i=0; i<5; i++)
            System.out.println(args[i]);
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centers = new Path(input.getParent().toString() + "centersFile");

        configuration.set("centersFilePath", centers.toString());
        configuration.setDouble("conv_threshold", Double.parseDouble(args[4]));

        int k = Integer.parseInt(args[2]);
        configuration.setInt("k", k);
        int numDimensions = Integer.parseInt(args[3]);
        configuration.setInt("numDimensions", numDimensions);

        Job job;
        
        FileSystem fs = FileSystem.get(output.toUri(),configuration);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + output.toString());
            fs.delete(output, true);
        }

        createCenters(k, configuration, centers);

        long stopCondition = 0;
        int iterations = 0;
        while (stopCondition != 1) {
            job = Job.getInstance(configuration, "K means iter");
            job.setJarByClass(Kmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setReducerClass(KmeansReducer.class);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.setMapOutputKeyClass(Center.class);
            job.setMapOutputValueClass(Point.class);

            job.waitForCompletion(true);

            stopCondition = job.getCounters().findCounter(KmeansReducer.CONVERGE_STATUS.CONVERGED).getValue();

            //if(stopCondition!=1)
            fs.delete(output, true);
            iterations++;
        }
        
        //task is completed
        endJob = System.currentTimeMillis();
        long TotTimeExecution = endJob - startJob;
        long TotTimeExecutionMin;
        long TotTimeExecutionSec;
        TotTimeExecutionMin = TimeUnit.MILLISECONDS.toMinutes(TotTimeExecution);
        TotTimeExecutionSec = TimeUnit.MILLISECONDS.toSeconds(TotTimeExecution)%60;

        //fs.delete(centers.getParent(), true);
        System.out.println("Number of iterations\t" + iterations);
        System.out.println("Execution time:  min." + String.valueOf(TotTimeExecutionMin)+" "+String.valueOf(TotTimeExecutionSec)+" sec.");
    }

    private static void createCenters(int k, Configuration configuration, Path centers) throws IOException {
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Center.class));
        Random r = new Random();
        List<DoubleWritable> listParameters = new ArrayList<DoubleWritable>();
        Center tempC;
        Double temp;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < configuration.getInt("numDimensions", 2); j++) {
                temp = Math.floor(100.0 * r.nextDouble() * 100) / 100;
                listParameters.add(new DoubleWritable(temp));
            }
            tempC = new Center(listParameters, new IntWritable(i), new IntWritable(0));
            centerWriter.append(new IntWritable(i), tempC);
            listParameters = new ArrayList<DoubleWritable>();
        }
        centerWriter.close();
    }
}

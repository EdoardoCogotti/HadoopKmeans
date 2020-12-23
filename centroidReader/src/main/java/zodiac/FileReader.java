package zodiac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import java.io.IOException;
 
public class FileReader {
 public static void main(String[] args) {
  
  Configuration conf = new Configuration();
  try {
   Path inFile = new Path(args[0]);
   SequenceFile.Reader reader = null;
   try {
    IntWritable key = new IntWritable();
    Center value = new Center();
    reader = new SequenceFile.Reader(conf, Reader.file(inFile), Reader.bufferSize(4096));
    while(reader.next(key, value)) {
     System.out.println("Key " + key + "Value " + value);
    }
 
   }finally {
    if(reader != null) {
     reader.close();
    }
   }
  } catch (IOException e) {
   // TODO Auto-generated catch bloc
   e.printStackTrace();
  }
 }
}
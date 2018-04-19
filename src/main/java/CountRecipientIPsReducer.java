import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountRecipientIPsReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  Set setA = new HashSet();
	  
	  for (Text val : values) {
	     setA.add(val.toString());
	  }
	  
	  Text t = new Text();
	  t.set(key + ":");
	  //System.out.println(t.toString());
	  context.write(t, new IntWritable(setA.size()));
	  
  }
}
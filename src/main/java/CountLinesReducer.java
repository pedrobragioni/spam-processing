import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountLinesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  int sum = 0;
	  for (IntWritable val : values) {
	     sum += val.get();
	  }
	  
	  Text t = new Text();
	  t.set("Num. de linhas: ");
	  context.write(t, new IntWritable(sum));

  }
}

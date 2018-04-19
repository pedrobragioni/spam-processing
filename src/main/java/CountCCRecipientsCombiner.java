import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCCRecipientsCombiner extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  Set setA = new HashSet();
	  
	  for (Text val : values) {
	     setA.add(val);
	  }
	  
	  Text t = new Text();
      Iterator it = setA.iterator();
      while (it.hasNext()) {
         t.set(key + ":");
         //System.out.println(t.toString());
         context.write(t, (Text)it.next());
      }
  }
}

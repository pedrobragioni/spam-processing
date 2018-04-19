import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountLinesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text out = new Text();
    private IntWritable um = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
         out.set("");
         context.write(out , um);
         value = null;
	}	
}

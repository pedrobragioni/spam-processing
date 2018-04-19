package spam.counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import spam.Mail;

public class CountMessagesMapper extends Mapper<LongWritable, Mail, Text, IntWritable> {

	private Text out = new Text();

	@Override
	public void map(LongWritable key, Mail value, Context context)
			throws IOException, InterruptedException {
		
		Runtime runtime = Runtime.getRuntime();
		
		IntWritable um = new IntWritable(1);
		
		if(value.From != "Invalid"){
			out.set("");
			context.write(out , um);
			value = null;
		}
		
	}	
}

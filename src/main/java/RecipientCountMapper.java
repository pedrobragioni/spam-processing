import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import spam.Mail;

public class RecipientCountMapper extends Mapper<LongWritable, Mail, Text, IntWritable> {

	private Text out = new Text();

	@Override
	public void map(LongWritable key, Mail value, Context context)
			throws IOException, InterruptedException {
		
		IntWritable um = new IntWritable(1);
		//int count = 0;
		if(value.From != "Invalid"){
			Iterator it = value.Rcpt_to.iterator();
			while(it.hasNext()){
				//System.out.println(count++);
				out.set(it.next().toString());
				context.write(out , um);
			}
		}
		
	}	
}

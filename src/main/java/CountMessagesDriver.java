package spam.counter;

//import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import spam.Mail;
import spam.reader.PatternInputFormat;


public class CountMessagesDriver extends Configured implements Tool {

   private static final Logger LOGGER = Logger.getLogger(
         CountMessagesDriver.class.getName());

   @Override
   public int run(String[] args) throws Exception {

      String regex = "^From\\s.*\\s\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$";

      Configuration conf = this.getConf();
      conf.set("record.delimiter.regex", regex);

      LOGGER.info("mapreduce.job.reduces: " + conf.get("mapreduce.job.reduces"));
      LOGGER.info("OLD:mapred.reduce.tasks: " + conf.get("mapred.reduce.tasks"));

      Job job = new Job(conf);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(CountMessagesMapper.class);
      job.setReducerClass(CountMessagesReducer.class);
      job.setCombinerClass(CountMessagesReducer.class);

      job.setJarByClass(CountMessagesDriver.class);

      job.setInputFormatClass(PatternInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      int ret = job.waitForCompletion(true) ? 0 : 1;

      return ret;
   }
   
   public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new CountMessagesDriver(), args);
      System.exit(res);
   }
}

package spam.reader;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import spam.Mail;

public class PatternInputFormat
extends FileInputFormat<LongWritable, Mail>{

	@Override
	public RecordReader<LongWritable, Mail> createRecordReader(
			InputSplit split,
			TaskAttemptContext context)
					throws IOException,
					InterruptedException {
		
		return new PatternRecordReader();
	}

}

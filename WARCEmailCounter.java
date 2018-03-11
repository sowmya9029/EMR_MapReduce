package org.commoncrawl.examples.mapreduce;

import edu.cmu.lemurproject.WarcFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * HTML tag count example using the raw HTTP responses (WARC) from the Common Crawl dataset.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCEmailCounter extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WARCEmailCounter.class);

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job.
	 * @param args args
	 *             @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WARCEmailCounter(), args);
		System.exit(res);
	}

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//
		Job job = new Job(conf);
		job.setJarByClass(WARCEmailCounter.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(WarcFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(EmailCounterMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(10);
		return job.waitForCompletion(true) ? 0 : -1;
	}


	public static class MyReducer
			extends Reducer<Text,Text,Text,LongWritable> {
		private LongWritable result = new LongWritable();


		@Override
		public void reduce(Text key, Iterable<Text> values,
						   Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			Set<String> uniqueEmailsPerURL = new HashSet<>();
			for(Text text : values) {
				uniqueEmailsPerURL.add(text.toString());
			}
			result.set(uniqueEmailsPerURL.size());
			context.write(key, result);
		}
	}

}

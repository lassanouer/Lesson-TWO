package com.ov.training.secondEx.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ov.training.commons.BundelUtils;
import com.ov.training.commons.Constants;
import com.ov.training.secondEx.StubDriver;

public class JobBuider {

	static final Logger logger = LoggerFactory.getLogger(StubDriver.class);

	public static <T extends Mapper<?, ?, ?, ?>, V extends Reducer<?, ?, ?, ?>> int runJob(String jobName, Path path_1, Path path_2, Class<T> mapper, Class<V> reducer)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(StubDriver.class);
		job.setJobName(jobName);

		FileInputFormat.addInputPath(job, path_1);
		FileOutputFormat.setOutputPath(job, path_2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(mapper);
		job.setReducerClass(reducer);

		int returnValue = job.waitForCompletion(true) ? Constants.sZero : Constants.sUn;

		if (job.isSuccessful()) {
			logger.info("{}" + BundelUtils.get("msg.success.run.job"), jobName);
		} else if (!job.isSuccessful()) {
			logger.info("{}" + BundelUtils.get("msg.error.run.job"), jobName);
		}
		return returnValue;
	}
}

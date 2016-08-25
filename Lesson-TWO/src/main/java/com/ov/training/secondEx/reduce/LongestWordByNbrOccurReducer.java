package com.ov.training.secondEx.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author anouer.lassoued
 *
 */
public class LongestWordByNbrOccurReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	public void reduce(IntWritable ikey, Iterable<Text> iValues, Context iContext) throws IOException, InterruptedException {

		Text ltext = new Text();
		for (Text value : iValues) {
			if (ltext.getLength() < value.getLength()) {
				ltext.set(value);
			}
		}
		iContext.write(new Text(ltext), ikey);
	}
}

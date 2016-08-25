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
public class SumOccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text ikey, Iterable<IntWritable> iValues, Context iContext) throws IOException, InterruptedException {

		int lSum = 0;
		for (IntWritable lValue : iValues) {
			lSum += lValue.get();
		}
		iContext.write(ikey, new IntWritable(lSum));
	}

}

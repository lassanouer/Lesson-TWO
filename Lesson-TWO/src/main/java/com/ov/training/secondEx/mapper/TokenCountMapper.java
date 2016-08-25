package com.ov.training.secondEx.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author anouer.lassoued 
 * Count the number of occurrences of a word in a text file
 */
public class TokenCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable sOne = new IntWritable(1);
	private Text mWord = new Text();

	public void map(LongWritable ikey, Text ivalue, Context icontext) throws IOException, InterruptedException {
		String lline = ivalue.toString();
		StringTokenizer lst = new StringTokenizer(lline, " ");

		while (lst.hasMoreTokens()) {
			mWord.set(lst.nextToken());
			icontext.write(mWord, sOne);
		}
	}

}

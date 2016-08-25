package com.ov.training.secondEx.mapper;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author anouer.lassoued
 *
 * @param <K>
 *            key
 * @param <V>
 *            value
 */
public class InverseMapper<K, V> extends Mapper<K, V, V, K> {

	public void map(K ikey, V iValue, Context iContext) throws IOException, InterruptedException {
		iContext.write(iValue, ikey);
	}
}

package com.ov.training.secondEx;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ov.training.commons.BundelUtils;
import com.ov.training.commons.Constants;
import com.ov.training.secondEx.job.JobBuider;
import com.ov.training.secondEx.mapper.InverseMapper;
import com.ov.training.secondEx.mapper.TokenCountMapper;
import com.ov.training.secondEx.reduce.LongestWordByNbrOccurReducer;
import com.ov.training.secondEx.reduce.SumOccurrenceReducer;

/**
 * 
 * @author Anouer Lassoued
 *
 */
public class StubDriver extends Configured implements Tool {

	static final Logger logger = LoggerFactory.getLogger(StubDriver.class);

	/**
	 * Test if argements are valid
	 * 
	 * @param args
	 * @return
	 */
	public int testInputArguments(String[] args) {
		if (args.length != Constants.sDeux) {
			System.err.printf(BundelUtils.get("msg.error.argument.run.jar"), getClass().getSimpleName());
			return Constants.sMoinUn;
		}
		return Constants.sZero;
	}

	public int run(String[] args) throws Exception {
		int result = Constants.sZero;
		if (testInputArguments(args) != Constants.sZero) {
			return Constants.sMoinUn;
		}

		// Go for the first Job
		result = JobBuider.runJob(BundelUtils.get("name.first.job"), new Path(args[Constants.sZero]),
				new Path(args[Constants.sUn] + BundelUtils.get("suffix.for.temp.file")), TokenCountMapper.class,
				SumOccurrenceReducer.class);
		if (result == Constants.sZero) {

			// Go for the second Job
			result = JobBuider.runJob(BundelUtils.get("name.second.job"),
					new Path(args[Constants.sUn] + BundelUtils.get("suffix.for.temp.file")),
					new Path(args[Constants.sUn]), InverseMapper.class, LongestWordByNbrOccurReducer.class);
			if (result != Constants.sZero) {
				logger.error("{}" + BundelUtils.get("msg.error.run.job"), BundelUtils.get("name.second.job"));
			}
		} else {
			logger.error("{}" + BundelUtils.get("msg.error.run.job"), BundelUtils.get("name.first.job"));
		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StubDriver(), args);
		System.exit(exitCode);
	}

}

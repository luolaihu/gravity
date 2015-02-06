package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.job;

import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.mapred.*;
import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.writable.EntityPrefWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.util.List;
import java.util.Map;

public class PreparePreferenceMatrixJob extends AbstractJob {
	
	public static final String NUM_USERS = "numUsers.bin";
	public static final String ITEMID_INDEX = "itemIDIndex";
	public static final String USER_VECTORS = "userVectors";
	public static final String RATING_MATRIX = "ratingMatrix";

	private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PreparePreferenceMatrixJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		addInputOption();
		addOutputOption();
		addOption("maxPrefsPerUser", "mppu", "max number of preferences to consider per user, " 
				+ "users with more preferences will be sampled down");
		addOption("maxPrefsPerItem",null,null);
  	  	addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this "
  	  			+ "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
  	  	addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
  	  	addOption("ratingShift", "rs", "shift ratings by this value", "0.0");

  	  	Map<String, List<String>> parsedArgs = parseArguments(args);
  	  	if (parsedArgs == null) {
  	  		return -1;
  	  	}

  	  	int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
  	  	boolean booleanData = Boolean.valueOf(getOption("booleanData"));
  	  	float ratingShift = Float.parseFloat(getOption("ratingShift"));
  	  	//convert items to an internal index
  	  	Job itemIDIndex = prepareJob(getInputPath(), getOutputPath(ITEMID_INDEX), TextInputFormat.class,
  	  			ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class, ItemIDIndexReducer.class,
  	  			VarIntWritable.class, VarLongWritable.class, SequenceFileOutputFormat.class);
  	  	itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);
  	  	boolean succeeded = itemIDIndex.waitForCompletion(true);
    	if (!succeeded) {
    		return -1;
    	}
    	
    	//convert user preferences into a vector per user
    	Job toUserVectors = prepareJob(getInputPath(), getOutputPath(USER_VECTORS), TextInputFormat.class,
    			ToItemPrefsMapper.class, VarLongWritable.class, booleanData ? VarLongWritable.class : EntityPrefWritable.class,
    					ToUserVectorsReducer.class, VarLongWritable.class, VectorWritable.class, SequenceFileOutputFormat.class);
    	toUserVectors.getConfiguration().setBoolean(RecommenderJob.BOOLEAN_DATA, booleanData);
    	toUserVectors.getConfiguration().setInt(ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
    	toUserVectors.getConfiguration().set(ToEntityPrefsMapper.RATING_SHIFT, String.valueOf(ratingShift));
    	succeeded = toUserVectors.waitForCompletion(true);
    	if (!succeeded) {
    		return -1;
    	}
    
    	//we need the number of users later
    	int numberOfUsers = (int) toUserVectors.getCounters().findCounter(ToUserVectorsReducer.Counters.USERS).getValue();
    	HadoopUtil.writeInt(numberOfUsers, getOutputPath(NUM_USERS), getConf());
    
    		//build the rating matrix
    	Job toItemVectors = prepareJob(getOutputPath(USER_VECTORS), getOutputPath(RATING_MATRIX),
    			ToItemVectorsMapper.class, IntWritable.class, VectorWritable.class, ToItemVectorsReducer.class,
    			IntWritable.class, VectorWritable.class);
    	toItemVectors.setCombinerClass(ToItemVectorsReducer.class);

    	/* configure sampling regarding the uservectors */
    	if (hasOption("maxPrefsPerUser")) {
    		int samplingSize = Integer.parseInt(getOption("maxPrefsPerUser"));
    		toItemVectors.getConfiguration().setInt(ToItemVectorsMapper.SAMPLE_SIZE, samplingSize);
    	}
    	
    	if(hasOption("maxPrefsPerItem")){
    		int samplingSize =  Integer.parseInt(getOption("maxPrefsPerItem"));
    		toItemVectors.getConfiguration().setInt(ToItemVectorsReducer.SAMPLE_SIZE,samplingSize);
    	}
    	
    	succeeded = toItemVectors.waitForCompletion(true);
    	if (!succeeded) {
    		return -1;
    	}
    	return 0;
	}
}
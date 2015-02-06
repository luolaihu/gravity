package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

public final class ItemIDIndexReducer extends
	Reducer<VarIntWritable, VarLongWritable, VarIntWritable,VarLongWritable> {
  
	@Override
	protected void reduce(VarIntWritable index,
     	                   Iterable<VarLongWritable> possibleItemIDs,
     	                   Context context) throws IOException, InterruptedException {
		long minimumItemID = Long.MAX_VALUE;
		for (VarLongWritable varLongWritable : possibleItemIDs) {
			long itemID = varLongWritable.get();
			if (itemID < minimumItemID) {
        	minimumItemID = itemID;
			}
   	 	}
		if (minimumItemID != Long.MAX_VALUE) {
			context.write(index, new VarLongWritable(minimumItemID));
		}
	}
}

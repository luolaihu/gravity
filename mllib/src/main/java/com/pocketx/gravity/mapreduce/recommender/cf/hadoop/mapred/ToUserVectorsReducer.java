package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.mapred;

import java.io.IOException;

import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util.TasteHadoopUtils;
import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.writable.EntityPrefWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public final class ToUserVectorsReducer extends
	Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable> {

	public static final String MIN_PREFERENCES_PER_USER = ToUserVectorsReducer.class.getName() 
			+ ".minPreferencesPerUser";

	private int minPreferences;

	public enum Counters { USERS }

	@Override
	protected void setup(Context ctx) throws IOException, InterruptedException {
		super.setup(ctx);
		minPreferences = ctx.getConfiguration().getInt(MIN_PREFERENCES_PER_USER, 1);
	}

	@Override
	protected void reduce(VarLongWritable userID,
			Iterable<VarLongWritable> itemPrefs,
			Context context) throws IOException, InterruptedException {
		Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
		for (VarLongWritable itemPref : itemPrefs) {
			int index = TasteHadoopUtils.idToIndex(itemPref.get());
			float value = itemPref instanceof EntityPrefWritable ? ((EntityPrefWritable) itemPref).getPrefValue() : 1.0f;
			userVector.set(index, value);
		}

		if (userVector.getNumNondefaultElements() >= minPreferences) {
			VectorWritable vw = new VectorWritable(userVector);
			vw.setWritesLaxPrecision(true);
			context.getCounter(Counters.USERS).increment(1);
			context.write(userID, vw);
   	 	}
	}
}

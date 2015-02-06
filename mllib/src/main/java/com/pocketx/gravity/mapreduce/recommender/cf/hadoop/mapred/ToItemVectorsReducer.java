package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.mapred;

import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util.Vectors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class ToItemVectorsReducer 
	extends Reducer<IntWritable,VectorWritable,IntWritable,VectorWritable> {
	public static final String SAMPLE_SIZE = ToItemVectorsReducer.class + ".sampleSize";
	private int sampleSize;
	protected void setup(Context context) throws IOException,InterruptedException{
		sampleSize = context.getConfiguration().getInt(SAMPLE_SIZE, Integer.MAX_VALUE);
	}
	
	@Override
	protected void reduce(IntWritable row, Iterable<VectorWritable> vectors, Context ctx)
			throws IOException, InterruptedException {
		
    	VectorWritable vectorWritable = VectorWritable.merge(vectors.iterator());
    	Vector vector = vectorWritable.get();
    	vector = Vectors.maybeSample(vector, sampleSize);
    	vectorWritable.set(vector);
    	vectorWritable.setWritesLaxPrecision(true);
    	ctx.write(row, vectorWritable);
  	}
}
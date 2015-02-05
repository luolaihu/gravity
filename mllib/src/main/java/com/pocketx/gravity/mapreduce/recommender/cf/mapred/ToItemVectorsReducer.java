package com.pocketx.gravity.mapreduce.recommender.cf.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;

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
    	//改动,使用sample
        Vector vector = vectorWritable.get();
    	vector = Vectors.maybeSample(vector, sampleSize);
    	vectorWritable.set(vector);
        //
    	vectorWritable.setWritesLaxPrecision(true);
    	ctx.write(row, vectorWritable);
  	}
}
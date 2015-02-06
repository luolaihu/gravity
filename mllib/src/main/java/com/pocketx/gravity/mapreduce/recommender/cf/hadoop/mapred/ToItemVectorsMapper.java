package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.mapred;

import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util.TasteHadoopUtils;
import com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util.Vectors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

public class ToItemVectorsMapper
	extends Mapper<VarLongWritable,VectorWritable,IntWritable,VectorWritable> {

	public static final String SAMPLE_SIZE = ToItemVectorsMapper.class + ".sampleSize";

	enum Elements {
		USER_RATINGS_USED, USER_RATINGS_NEGLECTED
 	 }

	private int sampleSize;

	@Override
	protected void setup(Context ctx) throws IOException, InterruptedException {
		sampleSize = ctx.getConfiguration().getInt(SAMPLE_SIZE, Integer.MAX_VALUE);
	}

	@Override
	protected void map(VarLongWritable rowIndex, VectorWritable vectorWritable, Context ctx)
			throws IOException, InterruptedException {
		Vector userRatings = vectorWritable.get();

		int numElementsBeforeSampling = userRatings.getNumNondefaultElements();
		userRatings = Vectors.maybeSample(userRatings, sampleSize);
		int numElementsAfterSampling = userRatings.getNumNondefaultElements();

		int column = TasteHadoopUtils.idToIndex(rowIndex.get());
		VectorWritable itemVector = new VectorWritable(new RandomAccessSparseVector(Integer.MAX_VALUE, 1));
		itemVector.setWritesLaxPrecision(true);

		Iterator<Vector.Element> iterator = userRatings.nonZeroes().iterator();
		while (iterator.hasNext()) {
			Vector.Element elem = iterator.next();
			itemVector.get().setQuick(column, elem.get());
			ctx.write(new IntWritable(elem.index()), itemVector);
    	}

		ctx.getCounter(Elements.USER_RATINGS_USED).increment(numElementsAfterSampling);
		ctx.getCounter(Elements.USER_RATINGS_NEGLECTED).increment(numElementsBeforeSampling - numElementsAfterSampling);
	}
}
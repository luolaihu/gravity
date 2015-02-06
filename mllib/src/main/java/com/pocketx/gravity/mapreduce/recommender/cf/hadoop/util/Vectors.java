package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.primitives.Doubles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntIntHashMap;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Vectors {

	private Vectors() {}

	public static Vector maybeSample(Vector original, int sampleSize) {
		if (original.getNumNondefaultElements() <= sampleSize) {
			return original;
		}
		Vector sample = original.like();
		
		/****************************************************新增*************************************************/
		List<Vector> tempList = new ArrayList<Vector>();
		int sofar = 0;
		Random random = RandomUtils.getRandom();
		Iterator<Vector.Element> oringalIterator = original.nonZeroes().iterator();
		while(oringalIterator.hasNext()){	
			Vector.Element e = oringalIterator.next();
			Vector vector =  new RandomAccessSparseVector(Integer.MAX_VALUE, 1);
			vector.setQuick(e.index(), e.get());
			sofar++;
			if(tempList.size() < sampleSize){
				tempList.add(vector);
			}
			else{
				  int position = random.nextInt(sofar);
				  if (position < tempList.size()) {
					  tempList.set(position, vector);
				  }
			}
		}
		
		Iterator<Vector> sampledElements =	tempList.iterator();
		
		/****************************************************新增*************************************************/
		while (sampledElements.hasNext()) {
			Vector vector= sampledElements.next();
			Iterator<Vector.Element> eIterator = vector.nonZeroes().iterator();
			while(eIterator.hasNext()){
				Vector.Element e = eIterator.next();
				sample.setQuick(e.index(), e.get());
			}
    	}
		
		return sample;
  	}
	
	public static Vector topKElements(int k, Vector original) {
		if (original.getNumNondefaultElements() <= k) {
			return original;
    	}
		TopK<Vector.Element> topKQueue = new TopK<Vector.Element>(k, BY_VALUE);
			Iterator<Vector.Element> nonZeroElements = original.nonZeroes().iterator();
			while (nonZeroElements.hasNext()) {
				Vector.Element nonZeroElement = nonZeroElements.next();
				topKQueue.offer(new TemporaryElement(nonZeroElement));
			}
			Vector topKSimilarities = original.like();
			for (Vector.Element topKSimilarity : topKQueue.retrieve()) {
				topKSimilarities.setQuick(topKSimilarity.index(), topKSimilarity.get());
			}
			return topKSimilarities;
	}

	public static Vector merge(Iterable<VectorWritable> partialVectors) {
		Iterator<VectorWritable> vectors = partialVectors.iterator();
		Vector accumulator = vectors.next().get();
		while (vectors.hasNext()) {
			VectorWritable v = vectors.next();
			if (v != null) {
				Iterator<Vector.Element> nonZeroElements = v.get().nonZeroes().iterator();
				while (nonZeroElements.hasNext()) {
					Vector.Element nonZeroElement = nonZeroElements.next();
					accumulator.setQuick(nonZeroElement.index(), nonZeroElement.get());
				}
			}
		}
		return accumulator;
	}

	public static final Comparator<Vector.Element> BY_VALUE = new Comparator<Vector.Element>() {
		@Override
		public int compare(Vector.Element elem1, Vector.Element elem2) {
			return Doubles.compare(elem1.get(), elem2.get());
		}
	};

	public static class TemporaryElement implements Vector.Element {

		private final int index;
		private double value;

		public TemporaryElement(int index, double value) {
			this.index = index;
			this.value = value;
		}

		public TemporaryElement(Vector.Element toClone) {
			this(toClone.index(), toClone.get());
		}

		@Override
		public double get() {
			return value;
		}

		@Override
		public int index() {
			return index;
		}

		@Override
		public void set(double value) {
			this.value = value;
		}
	}

	public static Vector.Element[] toArray(VectorWritable vectorWritable) {
		Vector.Element[] elements = new Vector.Element[vectorWritable.get().getNumNondefaultElements()];
		int k = 0;
		Iterator<Vector.Element> nonZeroElements = vectorWritable.get().nonZeroes().iterator();
		while (nonZeroElements.hasNext()) {
			Vector.Element nonZeroElement = nonZeroElements.next();
			elements[k++] = new TemporaryElement(nonZeroElement.index(), nonZeroElement.get());
		}
		return elements;
	}

	public static void write(Vector vector, Path path, Configuration conf) throws IOException {
		write(vector, path, conf, false);
  	}

	public static void write(Vector vector, Path path,
			Configuration conf, boolean laxPrecision) throws IOException {
		
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		FSDataOutputStream out = fs.create(path);
		try {
			VectorWritable vectorWritable = new VectorWritable(vector);
			vectorWritable.setWritesLaxPrecision(laxPrecision);
			vectorWritable.write(out);
		} finally {
			Closeables.close(out,false);
		}
	}

	public static OpenIntIntHashMap readAsIntMap(Path path, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		FSDataInputStream in = fs.open(path);
		try {
			return readAsIntMap(in);
		} finally {
			Closeables.close(in,false);
		}
	}

	/* ugly optimization for loading sparse vectors containing ints only */
	public static OpenIntIntHashMap readAsIntMap(DataInput in) throws IOException {
		int flags = in.readByte();
		Preconditions.checkArgument(flags >>
			VectorWritable.NUM_FLAGS == 0, "Unknown flags set: %d", Integer.toString(flags, 2));
		boolean dense = (flags & VectorWritable.FLAG_DENSE) != 0;
		boolean sequential = (flags & VectorWritable.FLAG_SEQUENTIAL) != 0;
		boolean laxPrecision = (flags & VectorWritable.FLAG_LAX_PRECISION) != 0;
		Preconditions.checkState(!dense && !sequential, "Only for reading sparse vectors!");

		Varint.readUnsignedVarInt(in);

		OpenIntIntHashMap values = new OpenIntIntHashMap();
		int numNonDefaultElements = Varint.readUnsignedVarInt(in);
		for (int i = 0; i < numNonDefaultElements; i++) {
			int index = Varint.readUnsignedVarInt(in);
			double value = laxPrecision ? in.readFloat() : in.readDouble();
			values.put(index, (int) value);
		}
    	return values;
	}

	public static Vector read(Path path, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		FSDataInputStream in = fs.open(path);
		try {
			return VectorWritable.readVector(in);
		} finally {
			Closeables.close(in,false);
		}
	}
}

package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.VarLongWritable;

public final class EntityPrefWritable extends VarLongWritable {
  
	private float prefValue;
  
	public EntityPrefWritable() {
		// do nothing
	}
  
	public EntityPrefWritable(long itemID, float prefValue) {
		super(itemID);
		this.prefValue = prefValue;
	}
  
	public EntityPrefWritable(EntityPrefWritable other) {
		this(other.get(), other.getPrefValue());
	}

	public long getID() {
		return get();
	}

	public float getPrefValue() {
		return prefValue;
	}

	public void set(long id, float prefValue) {
		set(id);
		this.prefValue = prefValue;
	}
  
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeFloat(prefValue);
	}
  
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		prefValue = in.readFloat();
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ RandomUtils.hashFloat(prefValue);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof EntityPrefWritable)) {
			return false;
		}
		EntityPrefWritable other = (EntityPrefWritable) o;
		return get() == other.get() && prefValue == other.getPrefValue();
	}

	@Override
	public String toString() {
		return get() + "\t" + prefValue;
	}

	@Override
	public EntityPrefWritable clone() {
		return new EntityPrefWritable(get(), prefValue);
	}
}

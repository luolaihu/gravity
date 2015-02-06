package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Lists;
import org.apache.mahout.common.RandomUtils;

/**
 * Sample a fixed number of elements from an Iterator. The results can appear in any order.
 */
public final class FixedSizeSamplingIterator<T> extends ForwardingIterator<T> {

  private final Iterator<T> delegate;
  
  public FixedSizeSamplingIterator(int size, Iterator<T> source) {
    List<T> buf = Lists.newArrayListWithCapacity(size);
    int sofar = 0;
    Random random = RandomUtils.getRandom();
    while (source.hasNext()) {
      T v = source.next();
      sofar++;
      if (buf.size() < size) {
        buf.add(v);
      } else {
        int position = random.nextInt(sofar);
        if (position < buf.size()) {
          buf.set(position, v);
        }
      }
    }
    delegate = buf.iterator();
  }

  @Override
  protected Iterator<T> delegate() {
    return delegate;
  }
}
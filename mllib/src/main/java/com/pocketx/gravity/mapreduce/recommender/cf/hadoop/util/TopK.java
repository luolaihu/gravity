package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import java.util.Collections;
import java.util.Comparator;

/**
 * this class will preserve the k maximum elements of all elements it has been offered
 */
public class TopK<T> extends FixedSizePriorityQueue<T> {

  public TopK(int k, Comparator<? super T> comparator) {
    super(k, comparator);
  }

  @Override
  protected Comparator<? super T> queueingComparator(Comparator<? super T> stdComparator) {
    return stdComparator;
  }

  @Override
  protected Comparator<? super T> sortingComparator(Comparator<? super T> stdComparator) {
    return Collections.reverseOrder(stdComparator);
  }

  public T smallestGreat() {
    return peek();
  }
}

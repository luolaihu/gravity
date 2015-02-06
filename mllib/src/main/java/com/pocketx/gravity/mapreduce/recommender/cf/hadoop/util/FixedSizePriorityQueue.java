package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * base class for queues holding the top or min k elements of all elements they have been offered
 */
abstract class FixedSizePriorityQueue<T> {
	
	private final int k;
	private final Comparator<? super T> queueingComparator;
	private final Comparator<? super T> sortingComparator;
	private final Queue<T> queue;

	FixedSizePriorityQueue(int k, Comparator<? super T> comparator) {
		Preconditions.checkArgument(k > 0);
		this.k = k;
		Preconditions.checkNotNull(comparator);
		this.queueingComparator = queueingComparator(comparator);
		this.sortingComparator = sortingComparator(comparator);
		this.queue = new PriorityQueue<T>(k + 1, queueingComparator);
	}

	abstract Comparator<? super T> queueingComparator(Comparator<? super T> stdComparator);
	abstract Comparator<? super T> sortingComparator(Comparator<? super T> stdComparator);

	public void offer(T item) {
		if (queue.size() < k) {
			queue.add(item);
		} else if (queueingComparator.compare(item, queue.peek()) > 0) {
			queue.add(item);
			queue.poll();
		}
	}

	public boolean isEmpty() {
		return queue.isEmpty();
	}

	public int size() {
		return queue.size();
	}

	public List<T> retrieve() {
		List<T> topItems = Lists.newArrayList(queue);
		Collections.sort(topItems, sortingComparator);
		return topItems;
	}

	protected T peek() {
		return queue.peek();
	}
}
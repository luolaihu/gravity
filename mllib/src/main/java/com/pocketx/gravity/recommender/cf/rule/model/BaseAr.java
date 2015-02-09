package com.pocketx.gravity.recommender.cf.rule.model;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

/**
 * @author zhongxiaodong
 * @version created 2013-7-8
 */
public abstract class BaseAr extends AbstractJob {
	protected Integer minSupport		= 3;
	protected Integer maxHeapSize		= 50;
	protected String splitterPattern	= PFPGrowth.SPLITTER.toString();
	
	public Integer getMaxHeapSize() {
		return maxHeapSize;
	}

	/**
	 * @param maxHeapSize Default Value: 50
	 */
	public void setMaxHeapSize(Integer maxHeapSize) {
		this.maxHeapSize = maxHeapSize;
	}

	public Integer getMinSupport() {
		return minSupport;
	}

	/**
	 * @param minSupport Default Value: 3
	 */
	public void setMinSupport(Integer minSupport) {
		this.minSupport = minSupport;
	}

	public String getSplitterPattern() {
		return splitterPattern;
	}

	/**
	 * @param splitterPattern Default Value: "[ ,\t]*[,|\t][ ,\t]*"
	 */
	public void setSplitterPattern(String splitterPattern) {
		this.splitterPattern = splitterPattern;
	}
	
	/**
	 * generate frequent patterns using setting algorithm
	 * 
	 * @param input			path of transaction file in hdfs
	 * @param output		path of result in hdfs
	 * @param conf			hdfs configuration
	 * @throws Exception
	 */
	public abstract void generateFrequentPatterns(String input, String output, Configuration conf) throws Exception;
	
	/**
	 * read frequent patterns generate by generateFrequentPatterns function
	 * 
	 * @param output	the output path of generateFrequentPatterns function
	 * @param conf
	 * @return
	 * @throws java.io.IOException
	 */
	public abstract List<Pair<String,TopKStringPatterns>> readFrequentPatterns(String output, Configuration conf) throws IOException;
	
	/**
	 * read frequent list of items
	 * 
	 * @param output		the output path of generateFrequentPatterns function
	 * @param minSupport
	 * @return
	 */
	public List<Pair<String, Long>> readFrequentList(String output, String minSupport){
		return null;
	}
}
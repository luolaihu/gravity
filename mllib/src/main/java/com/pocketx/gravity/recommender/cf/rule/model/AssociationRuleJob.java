package com.pocketx.gravity.recommender.cf.rule.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pocketx.gravity.recommender.cf.rule.model.binpatternAr.BinaryPatternAr;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

/**
 * base class of binary relation ship calculate by association rule
 * 
 * @author zhongxiaodong
 * @version created 2013-8-1
 */
public class AssociationRuleJob {
	private int minSupport 				= 2;
	private String splitterPattern		= "[\\ ]";
	private BaseAr associationRule 		= new BinaryPatternAr(); /*new FpGrowthAr()*/;
	
	public int getMinSupport() {
		return minSupport;
	}
	public void setMinSupport(int minSupport) {
		this.minSupport = minSupport;
	}
	
	public String getSplitterPattern() {
		return splitterPattern;
	}
	public void setSplitterPattern(String splitterPattern) {
		this.splitterPattern = splitterPattern;
	}
	
	/**
	 * default association rule algorithm is FpGrowth
	 * 
	 * @param associationRule
	 */
	protected void setAssociationRule(BaseAr associationRule) {
		this.associationRule = associationRule;
	}
	
	/**
	 * generate related items using hadoop
	 * 
	 * @param input			item transaction path in hdfs
	 * @param output		frequent patterns save path in hdfs
	 * @throws Exception
	 */
	public void generateRelatedItems(String input, String output,
			Configuration conf) throws Exception {
		// configure
		associationRule.setMinSupport(minSupport);
		associationRule.setSplitterPattern(splitterPattern);

		// run association rule algorithm
		associationRule.generateFrequentPatterns(input, output, conf);
	}
	
	/**
	 * read binary related items after generate related items 
	 * 
	 * @param output	output path of generateRelatedItems function
	 * @return
	 * @throws java.io.IOException
	 */
	protected List<Pair<String, List<Pair<String, Long>>>> readBinaryRelatedItems(
			String output, Configuration conf) throws IOException {
		List<Pair<String, TopKStringPatterns>> frequencePatterns = associationRule
				.readFrequentPatterns(output, conf);
		List<Pair<String, List<Pair<String, Long>>>> relatedItems = new ArrayList<Pair<String, List<Pair<String, Long>>>>();

		for (Pair<String, TopKStringPatterns> frequentPattern : frequencePatterns) {
			String lItem = frequentPattern.getFirst();
			TopKStringPatterns topKPatterns = frequentPattern.getSecond();
			Set<String> items = new HashSet<String>();
			List<Pair<String, Long>> subRelatedItems = new ArrayList<Pair<String, Long>>();

			for (Pair<List<String>, Long> pattern : topKPatterns.getPatterns()) {
				List<String> rItems = pattern.getFirst();
				Long support = pattern.getSecond();

				for (String item : rItems) {
					if (item.compareTo(lItem) == 0 || items.contains(item)) {
						continue;
					}

					subRelatedItems.add(new Pair<String, Long>(item, support));
					items.add(item);
				}
			}

			if (subRelatedItems.size() > 0) {
				relatedItems.add(new Pair<String, List<Pair<String, Long>>>(
						lItem, subRelatedItems));
			}
		}

		return relatedItems;
	}
	
	/**
	 * read frequent map after generate related items 
	 * 
	 * @param output 	output path of generateRelatedItems function
	 * @return
	 * @throws java.io.IOException
	 */
	public Map<String, Long> readFrequentMap(String output)
			throws IOException {
		Map<String, Long> fMap = new HashMap<String, Long>();
		List<Pair<String, Long>> fList = associationRule.readFrequentList(
				output, Integer.toString(minSupport));

		for (Pair<String, Long> frequence : fList) {
			fMap.put(frequence.getFirst(), frequence.getSecond());
		}

		return fMap;
	}
}

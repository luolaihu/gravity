package com.pocketx.gravity.recommender.cf.rule.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.pocketx.gravity.common.DfConstant;
import com.pocketx.gravity.recommender.cf.rule.model.AssociationRuleJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;


public class RelatedRulesGenerateReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	private static final String		MIN_SUPPORT				= "minSupport";
	private static final String 	MIN_CONFIDENCE			= "minConfidence";
	private static final String 	MIN_LOYALTY				= "minLoyalty";
	
	private static final String 	EXCEPTION_ITEMS			= "exceptionItems";
	
	private double minConfidence = 0.01;
	private double minLoyalty = 0.01;
	private int maxRecSize = 100;
	
	private List<String> exceptionItems = new ArrayList<String>();
	private Map<String, Long> fMap = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		// get parameters
		minConfidence = Double.parseDouble(context.getConfiguration().get(
				MIN_CONFIDENCE));
		minLoyalty = Double.parseDouble(context.getConfiguration().get(
				MIN_LOYALTY));
		maxRecSize = context.getConfiguration().getInt("MAX_REC_SIZE", 100);

		// load exception items
		String items = context.getConfiguration().get(EXCEPTION_ITEMS);
		if (null != items && items.length() > 0) {
			String[] itemsArray = items.split(",");

			for (String item : itemsArray) {
				exceptionItems.add(item);
			}
		}

		// load frequent map
		int minSupport = context.getConfiguration().getInt(MIN_SUPPORT, 3);
		String freqPatternPath = context.getConfiguration().get(
				"freqPatternPath");
		fMap = readFMap(minSupport, freqPatternPath);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String lItem = key.toString();
		Long lItemFreq = fMap.get(lItem);
		List<Pair<String, Double>> topKPatterns = new ArrayList<Pair<String, Double>>();

		// calculate rules
		for (Text value : values) {
			String[] rItems = value.toString().split(",");
			String rItem = rItems[0];
			Long support = Long.parseLong(rItems[1]);
			Long recTimes = Long.parseLong(rItems[2]);
			Long rItemFreq = fMap.get(rItem);

			// calculate confidence and loyalty
			double confidence = (double) support / (double) lItemFreq;
			double loyalty = (double) support
					/ (double) (rItemFreq + lItemFreq - support);

			// filter
			if (needFilter(lItem, rItem) || confidence < minConfidence
					|| loyalty < minLoyalty) {
				continue;
			}

			// TF-IDF
			double IDF = Math.log((double) fMap.size() / (double) recTimes);
			double score = calculateScore(support, IDF, loyalty);

			topKPatterns.add(new Pair<String, Double>(rItem, score));
		}

		// sorting and cut off
		if (topKPatterns.size() > maxRecSize) {
			Collections.sort(topKPatterns,
					new Comparator<Pair<String, Double>>() {

						@Override
						public int compare(Pair<String, Double> p1,
								Pair<String, Double> p2) {
							return p2.getSecond().compareTo(p1.getSecond());
						}

					});
			topKPatterns = topKPatterns.subList(0, maxRecSize);
		}

		// save to hdfs
		if (topKPatterns.size() > 0) {
			context.write(NullWritable.get(), new Text(lItem
					+ DfConstant.INTER_ITEM_SPLITTER
					+ generateRelatedItemList(topKPatterns)));
		}
	}
	
	private String generateRelatedItemList(
			List<Pair<String, Double>> topKPatterns) {
		StringBuilder relatedItemList = new StringBuilder();

		for (Pair<String, Double> pattern : topKPatterns) {
			String rItem = pattern.getFirst();
			Double score = pattern.getSecond();

			relatedItemList.append(DfConstant.INNER_ITEM_SPLITTER);
			relatedItemList.append(rItem + DfConstant.SCORE_SPLITTER
					+ score.toString());
		}

		return relatedItemList.length() > DfConstant.INNER_ITEM_SPLITTER
				.length() ? relatedItemList
				.substring(DfConstant.INNER_ITEM_SPLITTER.length())
				: relatedItemList.toString();
	}

	private boolean needFilter(String lItem, String rItem) {
		return exceptionItems.contains(rItem)
				&& !exceptionItems.contains(lItem);
	}
	
	/**
	 * read frequent map just for PFPGrowth
	 * 
	 * @param minSupport
	 * @param relatedItemPath
	 * @return
	 * @throws java.io.IOException
	 */
	private Map<String, Long> readFMap(int minSupport, String relatedItemPath)
			throws IOException {
		AssociationRuleJob binRelatedAr = new AssociationRuleJob();

		// configure
		binRelatedAr.setMinSupport(minSupport);

		return binRelatedAr.readFrequentMap(relatedItemPath);
	}
	
	protected Double calculateScore(long support, double IDF, double loyalty) {
		return Math.log(1 + Math.pow((double) support, 0.5) * IDF * loyalty);
	}
}
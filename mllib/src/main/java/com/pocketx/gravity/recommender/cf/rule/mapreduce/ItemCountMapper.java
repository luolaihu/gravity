package com.pocketx.gravity.recommender.cf.rule.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

public class ItemCountMapper extends
		Mapper<Text, TopKStringPatterns, Text, Text> {

	@Override
	public void map(Text key, TopKStringPatterns value, Context context)
			throws IOException, InterruptedException {
		List<Pair<List<String>, Long>> patterns = value.getPatterns();
		Set<String> rItems = new HashSet<String>();
		String lItem = key.toString();

		// split
		for (Pair<List<String>, Long> pattern : patterns) {
			List<String> items = pattern.getFirst();
			long support = pattern.getSecond();

			for (String item : items) {
				// checking & filter
				if (lItem.equals(item) || rItems.contains(item)) {
					continue;
				}
				rItems.add(item);

				context.write(new Text(item),
						new Text(lItem + "," + Long.toString(support)));
			}
		}
	}
}

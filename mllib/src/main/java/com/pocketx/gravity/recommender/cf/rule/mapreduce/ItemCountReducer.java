package com.pocketx.gravity.recommender.cf.rule.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemCountReducer extends Reducer<Text, Text, NullWritable, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> items = new ArrayList<String>();
		String rItem = key.toString();
		Long quantity = 0L;

		// counting
		for (Text value : values) {
			items.add(value.toString());
			quantity++;
		}

		// write back
		for (String item : items) {
			String lItem = item.split(",")[0];
			String support = item.split(",")[1];

			context.write(NullWritable.get(), new Text(lItem + "_" + rItem
					+ "," + support + "," + quantity.toString()));
		}
	}
}

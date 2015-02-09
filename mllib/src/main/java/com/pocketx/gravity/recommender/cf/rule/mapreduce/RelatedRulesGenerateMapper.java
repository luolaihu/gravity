package com.pocketx.gravity.recommender.cf.rule.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelatedRulesGenerateMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String lItem = value.toString().split("_")[0];
		String rItemInfo = value.toString().split("_")[1];

		context.write(new Text(lItem), new Text(rItemInfo));
	}
}

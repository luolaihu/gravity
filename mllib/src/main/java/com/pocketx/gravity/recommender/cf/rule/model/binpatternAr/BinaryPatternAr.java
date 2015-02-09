package com.pocketx.gravity.recommender.cf.rule.model.binpatternAr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.pocketx.gravity.recommender.cf.rule.model.BaseAr;
import com.pocketx.gravity.recommender.cf.rule.model.fpgrowth.CopyFpGrowth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

public class BinaryPatternAr extends BaseAr {

	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();

		// load args
		if (parseArguments(args) == null) {
			return -1;
		}

		// clear output
		HadoopUtil.delete(getConf(), getOutputPath());

		// calculate
		startParallelCounting(getInputPath().toString(), getOutputPath()
				.toString());
		startParallelBinaryPatternAr(getInputPath().toString(), getOutputPath()
				.toString());

		return 0;
	}

	@Override
	public void generateFrequentPatterns(String input, String output,
			Configuration conf) throws Exception {
		String[] args = new String[4];
		// build args
		args[0] = "--input";
		args[1] = input;
		args[2] = "--output";
		args[3] = output;
		// generate frequent patterns
		ToolRunner.run(conf, this, args);

	}

	@Override
	public List<Pair<String, TopKStringPatterns>> readFrequentPatterns(
			String output, Configuration conf) throws IOException {
		Parameters params = new Parameters();

		// build parameters
		params.set(CopyFpGrowth.OUTPUT, output);

		return CopyFpGrowth.readFrequentPattern(params);
	}

	@Override
	public List<Pair<String, Long>> readFrequentList(String output,
			String minSupport) {
		Parameters params = new Parameters();

		// build parameters
		params.set(CopyFpGrowth.OUTPUT, output);
		params.set(CopyFpGrowth.MIN_SUPPORT, minSupport);

		return CopyFpGrowth.readFList(params);
	}

	public void startParallelCounting(String input, String output)
			throws IOException, InterruptedException, ClassNotFoundException {
		Parameters params = new Parameters();
		Configuration conf = getConf();

		// configure parameters
		params.set(CopyFpGrowth.INPUT, input);
		params.set(CopyFpGrowth.OUTPUT, output);
		params.set(CopyFpGrowth.SPLIT_PATTERN, getSplitterPattern());

		// setting splitter
		conf.set(PFPGrowth.PFP_PARAMETERS, params.toString());

		CopyFpGrowth.startParallelCounting(params, conf);
	}

	public void startParallelBinaryPatternAr(String input, String output)
			throws Exception {
		Path inputPath = new Path(input);
		Path outputPath = new Path(output, CopyFpGrowth.FREQUENT_PATTERNS);
		Configuration conf = getConf();

		// configure
		conf.set(CopyFpGrowth.SPLIT_PATTERN, getSplitterPattern());
		conf.set(CopyFpGrowth.MIN_SUPPORT, getMinSupport().toString());
		conf.set(CopyFpGrowth.MAX_HEAPSIZE, getMaxHeapSize().toString());

		// build job
		Job parallelBinaryPatternArJob = prepareJob(inputPath, outputPath,
				TextInputFormat.class, ParallelBinaryPatternArMapper.class,
				Text.class, Text.class, ParallelBinaryPatternArReducer.class,
				Text.class, TopKStringPatterns.class,
				SequenceFileOutputFormat.class);

		// execute
		if (!parallelBinaryPatternArJob.waitForCompletion(true)) {
			throw new Exception("parallel binary patterns job failed!");
		}
	}

	public static class ParallelBinaryPatternArMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Pattern splitter;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			splitter = Pattern.compile(context.getConfiguration().get(
					PFPGrowth.SPLIT_PATTERN, PFPGrowth.SPLITTER.toString()));
		}

		@Override
		protected void map(LongWritable offset, Text input, Context context)
				throws IOException, InterruptedException {
			String[] items = splitter.split(input.toString());

			// group by item
			for (int pos = 0; pos < items.length; pos++) {
				String item = items[pos];

				// A=>B also means B=>A if ignore confidence
				context.write(new Text(item),
						new Text(generateItemList(items, pos)));
			}
		}
		
		private String generateItemList(String[] items, int exception) {
			StringBuilder itemList = new StringBuilder();

			for (int pos = 0; pos < items.length; pos++) {
				if (pos == exception) {
					continue;
				}

				// build item list
				String item = items[pos];

				itemList.append(",");
				itemList.append(item);
			}

			return itemList.length() > ",".length() ? itemList.substring(","
					.length()) : itemList.toString();
		}
	}

	public static class ParallelBinaryPatternArReducer extends
			Reducer<Text, Text, Text, TopKStringPatterns> {
		private int minSupport;
		private int maxHeapSize;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			minSupport = Integer.parseInt(context.getConfiguration().get(
					CopyFpGrowth.MIN_SUPPORT, "2"));
			maxHeapSize = Integer.parseInt(context.getConfiguration().get(
					CopyFpGrowth.MAX_HEAPSIZE, "50"));
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String lItem = key.toString();
			Map<String, Long> patterns = new HashMap<String, Long>();
			List<Pair<List<String>, Long>> topKStringPatterns = new ArrayList<Pair<List<String>, Long>>();

			// statistic support
			for (Text value : values) {
				String[] rItems = value.toString().split(",");
				
				for (String rItem : rItems) {
					Long support;

					if ((support = patterns.get(rItem)) == null) {
						support = new Long(0);
					}
					patterns.put(rItem, ++support);
				}
			}

			// filter by minSupport
			for (Entry<String, Long> entry : patterns.entrySet()) {
				String rItem = entry.getKey();
				Long support = entry.getValue();

				if (minSupport > support) {
					continue;
				}

				// build patterns
				List<String> pattern = new ArrayList<String>();
				pattern.add(rItem);
				topKStringPatterns.add(new Pair<List<String>, Long>(pattern,
						support));
			}

			// sort
			Collections.sort(topKStringPatterns,
					new Comparator<Pair<List<String>, Long>>() {

						@Override
						public int compare(Pair<List<String>, Long> pattern0,
								Pair<List<String>, Long> pattern1) {
							Long s0 = pattern0.getSecond();
							Long s1 = pattern1.getSecond();

							return s1 - s0 > 0 ? 1 : s1 - s0 < 0 ? -1 : 0;
						}
					});
			if (topKStringPatterns.size() > maxHeapSize) {
				topKStringPatterns = topKStringPatterns.subList(0, maxHeapSize);
			}

			context.write(new Text(lItem), new TopKStringPatterns(
					topKStringPatterns));
		}
	}
}

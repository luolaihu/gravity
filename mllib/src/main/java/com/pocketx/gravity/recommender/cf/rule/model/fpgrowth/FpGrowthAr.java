package com.pocketx.gravity.recommender.cf.rule.model.fpgrowth;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.pocketx.gravity.recommender.cf.rule.model.BaseAr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.common.iterator.StringRecordIterator;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.convertors.SequenceFileOutputCollector;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.StringOutputConverter;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.apache.mahout.fpm.pfpgrowth.fpgrowth.FPGrowth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

/**
 * @author zhongxiaodong
 * @version created 2013-7-8
 */
public class FpGrowthAr extends BaseAr {
	
	public static final String SEQUENTIAL = "sequential";
	public static final String MAPREDUCE = "mapreduce";
	
	private static final String NUM_TREE_CACHE_ENTRIES = "numTreeCacheEntries";
	private static final String METHOD = "method";
	
	private static final Logger log = LoggerFactory.getLogger(FpGrowthAr.class);
	private static final Integer defaultMaxHeapSize = 50;
	private static final Integer defaultMinSupport = 3;
	
	private Integer numTreeCacheEntries = 7;
	private Integer numGroups	= 6000;
	private String method = "mapreduce";
	private String encoding = "UTF-8";
		
	public Integer getNumTreeCacheEntries() {
		return numTreeCacheEntries;
	}

	/**
	 * @param numTreeCacheEntries Default Value: 7 Recommended Values: [5-10]
	 */
	public void setNumTreeCacheEntries(Integer numTreeCacheEntries) {
		this.numTreeCacheEntries = numTreeCacheEntries;
	}

	public String getMethod() {
		return method;
	}

	/**
	 * @param method Method of processing: sequential|mapreduce, default Value: "mapreduce"
	 */
	public void setMethod(String method) {
		this.method = method;
	}

	public String getEncoding() {
		return encoding;
	}

	/**
	 * @param encoding Default Value: "UTF-8"
	 */
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public Integer getNumGroups() {
		return numGroups;
	}
	/**
	 * @param numGroups Default value: 6000
	 */
	public void setNumGroups(Integer numGroups) {
		this.numGroups = numGroups;
	}
	
	/**
	 * generate frequent patterns using mahout fp-growth
	 * 
	 * @param input 	input transaction path in hdfs
	 * @param output	output frequent patterns path in hdfs
	 * @param conf		hadoop configuration
	 * @throws Exception
	 */
	@Override
	public void generateFrequentPatterns(String input, String output, Configuration conf) throws Exception{
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
	public int run(String[] args) throws Exception {
	    addInputOption();
	    addOutputOption();

	    // setting parameters
	    addOption(CopyFpGrowth.MIN_SUPPORT, "s", "(Optional) The minimum number of times a co-occurrence must be present."
	              + " Default Value: 3", minSupport.toString());
	    addOption(CopyFpGrowth.MAX_HEAPSIZE, "k", "(Optional) Maximum Heap Size k, to denote the requirement to mine top K items."
	              + " Default value: 50", maxHeapSize.toString());
	    addOption(CopyFpGrowth.NUM_GROUPS, "g", "(Optional) Number of groups the features should be divided in the map-reduce version."
	              + " Doesn't work in sequential version Default Value:" + CopyFpGrowth.NUM_GROUPS_DEFAULT,
	              numGroups.toString());
	    addOption(CopyFpGrowth.SPLIT_PATTERN, "regex", "Regular Expression pattern used to split given string transaction into"
	            + " itemsets. Default value splits comma separated itemsets.  Default Value:"
	            + " \"[ ,\\t]*[,|\\t][ ,\\t]*\" ", splitterPattern);
	    addOption(NUM_TREE_CACHE_ENTRIES, "tc", "(Optional) Number of entries in the tree cache to prevent duplicate"
	            + " tree building. (Warning) a first level conditional FP-Tree might consume a lot of memory, "
	            + "so keep this value small, but big enough to prevent duplicate tree building. "
	            + "Default Value:5 Recommended Values: [5-10]", numTreeCacheEntries.toString());
	    addOption(METHOD, "method", "Method of processing: sequential|mapreduce", method);
	    addOption(CopyFpGrowth.ENCODING, "e", "(Optional) The file encoding.  Default value: UTF-8", encoding);
	    addFlag(CopyFpGrowth.USE_FPG2, "2", "Use an alternate FPG implementation");

		// load args by mahout
		if (parseArguments(args) == null) {
			return -1;
		}

		Parameters params = new Parameters();

		// configure parameters
		if (hasOption(CopyFpGrowth.MIN_SUPPORT)) {
			String minSupportString = getOption(CopyFpGrowth.MIN_SUPPORT);
			params.set(CopyFpGrowth.MIN_SUPPORT, minSupportString);
		}
		if (hasOption(CopyFpGrowth.MAX_HEAPSIZE)) {
			String maxHeapSizeString = getOption(CopyFpGrowth.MAX_HEAPSIZE);
			params.set(CopyFpGrowth.MAX_HEAPSIZE, maxHeapSizeString);
		}
		if (hasOption(CopyFpGrowth.NUM_GROUPS)) {
			String numGroupsString = getOption(CopyFpGrowth.NUM_GROUPS);
			params.set(CopyFpGrowth.NUM_GROUPS, numGroupsString);
		}

		if (hasOption(NUM_TREE_CACHE_ENTRIES)) {
			String numTreeCacheString = getOption(NUM_TREE_CACHE_ENTRIES);
			params.set("treeCacheSize", numTreeCacheString);
		}

		if (hasOption(CopyFpGrowth.SPLIT_PATTERN)) {
			String patternString = getOption(CopyFpGrowth.SPLIT_PATTERN);
			params.set(CopyFpGrowth.SPLIT_PATTERN, patternString);
		}

		String encoding = "UTF-8";
		if (hasOption(CopyFpGrowth.ENCODING)) {
			encoding = getOption(CopyFpGrowth.ENCODING);
		}
		params.set(CopyFpGrowth.ENCODING, encoding);

		if (hasOption(CopyFpGrowth.USE_FPG2)) {
			params.set(CopyFpGrowth.USE_FPG2, "true");
		}

		// set input path and out put path using the args
		Path inputDir = getInputPath();
		Path outputDir = getOutputPath();

		params.set(CopyFpGrowth.INPUT, inputDir.toString());
		params.set(CopyFpGrowth.OUTPUT, outputDir.toString());

		String classificationMethod = getOption(METHOD);
		if (SEQUENTIAL.equalsIgnoreCase(classificationMethod)) {
			runFPGrowth(params, getConf());
		} else if (MAPREDUCE.equalsIgnoreCase(classificationMethod)) {
			HadoopUtil.delete(getConf(), outputDir);
			CopyFpGrowth.runPFPGrowth(params, getConf());
		}

		return 0;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void runFPGrowth(Parameters params, Configuration conf) throws IOException {
	    log.info("Starting Sequential FPGrowth");
	    
	    // parameters configuration
	    int maxHeapSize = Integer.valueOf(params.get(CopyFpGrowth.MAX_HEAPSIZE, defaultMaxHeapSize.toString()));
	    int minSupport = Integer.valueOf(params.get(CopyFpGrowth.MIN_SUPPORT, defaultMinSupport.toString()));

	    String output = params.get(CopyFpGrowth.OUTPUT, "output.txt");

	    Path outputPath = new Path(output);
	    FileSystem fs = FileSystem.get(outputPath.toUri(), conf);

	    Charset encoding = Charset.forName(params.get(CopyFpGrowth.ENCODING));
	    String input = params.get(CopyFpGrowth.INPUT);

	    String pattern = params.get(CopyFpGrowth.SPLIT_PATTERN, CopyFpGrowth.SPLITTER.toString());

	    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, outputPath, Text.class, TopKStringPatterns.class);

	    // running pfpgrowth
	    if ("true".equals(params.get(CopyFpGrowth.USE_FPG2))) {
	      org.apache.mahout.fpm.pfpgrowth.fpgrowth2.FPGrowthObj<String> fp 
	        = new org.apache.mahout.fpm.pfpgrowth.fpgrowth2.FPGrowthObj<String>();
	      Collection<String> features = new HashSet<String>();

	      try {
	        fp.generateTopKFrequentPatterns(
	                new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
	                fp.generateFList(
	                        new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
	                        minSupport),
	                minSupport,
	                maxHeapSize,
	                features,
	                new StringOutputConverter(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer))
                    );//删除 new ContextStatusUpdater(null)
	      } finally {
	        Closeables.close(writer, false);
	      }
	    } else {
	      FPGrowth<String> fp = new FPGrowth<String>();
	      Collection<String> features = new HashSet<String>();
	      try {
	        fp.generateTopKFrequentPatterns(
	                new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
	                fp.generateFList(
	                        new StringRecordIterator(new FileLineIterable(new File(input), encoding, false), pattern),
	                        minSupport),
	                minSupport,
	                maxHeapSize,
	                features,
	                new StringOutputConverter(new SequenceFileOutputCollector<Text, TopKStringPatterns>(writer)),
	                new ContextStatusUpdater(null));
	      } finally {
	        Closeables.close(writer, false);
	      }
	    } 

	    /************************************************************************************************************
	    // read frequent patterns
	    List<Pair<String, TopKStringPatterns>> frequentPatterns = FPGrowth.readFrequentPattern(conf, outputPath);
	    for (Pair<String, TopKStringPatterns> entry : frequentPatterns) {
	      log.info("Dumping Patterns for Feature: {} \n{}", entry.getFirst(), entry.getSecond());
	    }
	    **************************************************************************************************************/
	  }
		
	/**
	 * read frequent patterns which generate by function generateFrequentPatterns (sequential)
	 * 
	 * @param frequentPatternPath	the path of frequent patterns calculate by generateFrequentPatterns function
	 * @param conf sequence file iterable configuration
	 * @return	the pairs of item and the top k frequent patterns of this item 
	 */
	public List<Pair<String,TopKStringPatterns>> readSequentialFrequentPatterns(String frequentPatternPath, Configuration conf){
		Path fpPath = new Path(frequentPatternPath);
		
		return FPGrowth.readFrequentPattern(conf, fpPath);
	}
	
	/**
	 * read frequent patterns which generate by function generateFrequentPatterns (mapreduce)
	 * 
	 * @param output the path of output of generateFrequentPatterns function
	 * @param conf
	 * @return the pairs of item and the top k frequent patterns of this item
	 * @throws java.io.IOException
	 */
	@Override
	public List<Pair<String,TopKStringPatterns>> readFrequentPatterns(String output, Configuration conf) throws IOException{
		Parameters params = new Parameters();
		
		// build parameters
		params.set(CopyFpGrowth.OUTPUT, output);
		
		return CopyFpGrowth.readFrequentPattern(params);
	}
	
	/**
	 * read frequent list which generate by function generateFrequentPatterns (mapreduce)
	 * 
	 * @param minSupport	minimum support
	 * @return	the pairs of items and support
	 */
	@Override
	public List<Pair<String, Long>> readFrequentList(String output, String minSupport){
		Parameters params = new Parameters();
		
		// build parameters
		params.set(CopyFpGrowth.OUTPUT, output);
		params.set(CopyFpGrowth.MIN_SUPPORT, minSupport);
		
		return CopyFpGrowth.readFList(params);
	}
}

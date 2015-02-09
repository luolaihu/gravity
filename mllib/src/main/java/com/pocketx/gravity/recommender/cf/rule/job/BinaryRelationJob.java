package com.pocketx.gravity.recommender.cf.rule.job;

import com.pocketx.gravity.recommender.cf.rule.mapreduce.ItemCountMapper;
import com.pocketx.gravity.recommender.cf.rule.mapreduce.ItemCountReducer;
import com.pocketx.gravity.recommender.cf.rule.mapreduce.RelatedRulesGenerateMapper;
import com.pocketx.gravity.recommender.cf.rule.mapreduce.RelatedRulesGenerateReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;

import java.io.IOException;

/**
 * Created by luolaihu1 on 2015/2/9.
 */
public class BinaryRelationJob extends AbstractJob {
    protected static final String MIN_SUPPORT = "minSupport";
    protected static final String MIN_CONFIDENCE = "minConfidence";
    protected static final String MIN_LOYALTY = "minLoyalty";

    protected static final String FREQUENT_PATTERN_PATH = "freqPatternPath";
    protected static final String EXCEPTION_ITEMS = "exceptionItems";

    protected double minConfidence = 0;
    protected double minLoyalty = 0;
    protected int minSupport = 1;

    protected int maxRecSize = 100;
    private String exceptionItems = null;

    @SuppressWarnings("rawtypes")
    private Class<? extends Reducer> relatedRulesReducer = RelatedRulesGenerateReducer.class;

    public double getMinConfidence() {
        return minConfidence;
    }

    public void setMinConfidence(double minConfidence) {
        this.minConfidence = minConfidence;
    }

    public double getMinLoyalty() {
        return minLoyalty;
    }

    public void setMinLoyalty(double minLoyalty) {
        this.minLoyalty = minLoyalty;
    }

    public String getExceptionItems() {
        return exceptionItems;
    }

    public void setExceptionItems(String exceptionItems) {
        this.exceptionItems = exceptionItems;
    }

    public int getMaxRecSize() {
        return maxRecSize;
    }

    public void setMaxRecSize(int maxRecSize) {
        this.maxRecSize = maxRecSize;
    }

    public int getMinSupport() {
        return minSupport;
    }

    public void setMinSupport(int minSupport) {
        this.minSupport = minSupport;
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends Reducer> getRelatedRulesReducer() {
        return relatedRulesReducer;
    }

    @SuppressWarnings("rawtypes")
    public void setRelatedRulesReducer(Class<? extends Reducer> relatedRulesReducer) {
        this.relatedRulesReducer = relatedRulesReducer;
    }


    public void generateRelationRules(String freqPatternPath,String itemDistributePath, String relatedItemPath)
            throws Exception {
        String[] args = new String[6];
        // build args
        args[0] = "--input";
        args[1] = freqPatternPath;
        args[2] = "--output";
        args[3] = relatedItemPath;
        args[4] = "--itemdistribute";
        args[5] = itemDistributePath;
        // generate related rules
        run(args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        addInputOption();
        addOutputOption();
        addOption("itemdistribute", "ids", "", "");
        Path frequentPatterns = new Path(getInputPath(), PFPGrowth.FREQUENT_PATTERNS);
        Path itemDistributePath = new Path(getOption("itemdistribute"));

        // delete item counter file
        try {
            HadoopUtil.delete(new Configuration(), itemDistributePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // count the item frequent in recommend result
        Job itemCountJob = prepareJob(frequentPatterns, itemDistributePath, SequenceFileInputFormat.class, ItemCountMapper.class, Text.class,
                Text.class, ItemCountReducer.class, NullWritable.class, Text.class, TextOutputFormat.class);
        itemCountJob.getConfiguration().setInt("mapred.reduce.tasks", 20);
        Boolean succeeded = itemCountJob.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }

        // delete output file
        try {
            HadoopUtil.delete(new Configuration(), getOutputPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TF-IDF ranking
        Job relatedRulesGenerateJob = prepareJob(itemDistributePath, getOutputPath(), TextInputFormat.class,
                RelatedRulesGenerateMapper.class, Text.class, Text.class, relatedRulesReducer, NullWritable.class, Text.class, TextOutputFormat.class);
        relatedRulesGenerateJob.getConfiguration().setInt("mapred.reduce.tasks", 20);
        relatedRulesGenerateJob.getConfiguration().setInt(MIN_SUPPORT, getMinSupport());
        relatedRulesGenerateJob.getConfiguration().set(MIN_CONFIDENCE, Double.toString(getMinConfidence()));
        relatedRulesGenerateJob.getConfiguration().set(MIN_LOYALTY, Double.toString(getMinLoyalty()));
        if (null != getExceptionItems()) {
            relatedRulesGenerateJob.getConfiguration().set(EXCEPTION_ITEMS, getExceptionItems());
        }
        relatedRulesGenerateJob.getConfiguration().setInt("MAX_REC_SIZE", getMaxRecSize());
        relatedRulesGenerateJob.getConfiguration().set("freqPatternPath", getInputPath().toString());
        succeeded = relatedRulesGenerateJob.waitForCompletion(true);
        if (!succeeded) {
            return -1;
        }
        return 0;
    }
}

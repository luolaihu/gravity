package com.pocketx.gravity.recommender.cf;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.RowSimilarityJob;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasures;
import org.apache.mahout.math.map.OpenIntLongHashMap;

/**
 * Created by luolaihu1 on 2015/2/3.
 */

public class SimilarJob extends AbstractJob{

        static final String ITEM_ID_INDEX_PATH_STR = SimilarJob.class.getName() + ".itemIDIndexPathStr";
        static final String MAX_SIMILARITIES_PER_ITEM = SimilarJob.class.getName() + ".maxSimilarItemsPerItem";

        private static final int DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM = 100;
        private static final int DEFAULT_MAX_PREFS_PER_USER = 1000;
        private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

        public static void main(String args[]) throws Exception{
            Configuration conf = new Configuration();
            conf.setInt("mapred.reduct.tasks", 20);
            ToolRunner.run(conf, new SimilarJob(), args);
        }
        @Override
        public int run(String[] args) throws Exception {


            addInputOption();
            addOutputOption();
            addOption("similarityClassname", "s", "Name of distributed similarity measures class to instantiate, "
                    + "alternatively use one of the predefined similarities (" + VectorSimilarityMeasures.list() + ')');
            addOption("maxSimilaritiesPerItem", "m", "try to cap the number of similar items per item to this number "
                            + "(default: " + DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM + ')',
                    String.valueOf(DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM));
            addOption("maxPrefsPerUser", "mppu", "max number of preferences to consider per user, "
                            + "users with more preferences will be sampled down (default: " + DEFAULT_MAX_PREFS_PER_USER + ')',
                    String.valueOf(DEFAULT_MAX_PREFS_PER_USER));
            addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this "
                    + "(default: " + DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
            addOption("booleanData", "b", "Treat input as without pref values", String.valueOf(Boolean.FALSE));
            addOption("threshold", "tr", "discard item pairs with a similarity value below this", false);

            Map<String,List<String>> parsedArgs = parseArguments(args);
            if (parsedArgs == null) {
                return -1;
            }



            String similarityClassName = getOption("similarityClassname");
            int maxSimilarItemsPerItem = Integer.parseInt(getOption("maxSimilaritiesPerItem"));
            int maxPrefsPerUser = Integer.parseInt(getOption("maxPrefsPerUser"));
            int minPrefsPerUser = Integer.parseInt(getOption("minPrefsPerUser"));
            boolean booleanData = Boolean.valueOf(getOption("booleanData"));

            double threshold = hasOption("threshold") ?
                    Double.parseDouble(getOption("threshold")) : RowSimilarityJob.NO_THRESHOLD;

            Path similarityMatrixPath = getTempPath("similarityMatrix");
            Path prepPath = getTempPath("prepareRatingMatrix");

            AtomicInteger currentPhase = new AtomicInteger();

            if (shouldRunNextPhase(parsedArgs, currentPhase)) {
                ToolRunner.run(getConf(), new PreparePreferenceMatrixJob(), new String[]{
                        "--input", getInputPath().toString(),
                        "--output", prepPath.toString(),
                        "--maxPrefsPerUser", String.valueOf(maxPrefsPerUser),
                        "--minPrefsPerUser", String.valueOf(minPrefsPerUser),
                        "--booleanData", String.valueOf(booleanData),
                        "--tempDir", getTempPath().toString() });
            }

            if (shouldRunNextPhase(parsedArgs, currentPhase)) {
                int numberOfUsers = HadoopUtil.readInt(new Path(prepPath, PreparePreferenceMatrixJob.NUM_USERS),
                        getConf());

                ToolRunner.run(getConf(), new RowSimilarityJob(), new String[] {
                        "--input", new Path(prepPath, PreparePreferenceMatrixJob.RATING_MATRIX).toString(),
                        "--output", similarityMatrixPath.toString(),
                        "--numberOfColumns", String.valueOf(numberOfUsers),
                        "--similarityClassname", similarityClassName,
                        "--maxSimilaritiesPerRow", String.valueOf(maxSimilarItemsPerItem),
                        "--excludeSelfSimilarity", String.valueOf(Boolean.TRUE),
                        "--threshold", String.valueOf(threshold),
                        "--tempDir", getTempPath().toString() });
            }

            if (shouldRunNextPhase(parsedArgs, currentPhase)) {
                Job mostSimilarItems = prepareJob(similarityMatrixPath, getOutputPath(), SequenceFileInputFormat.class,
                        MostSimilarItemPairsMapper.class, Text.class, DoubleWritable.class, TextOutputFormat.class);
                Configuration mostSimilarItemsConf = mostSimilarItems.getConfiguration();
                mostSimilarItemsConf.set(ITEM_ID_INDEX_PATH_STR,
                        new Path(prepPath, PreparePreferenceMatrixJob.ITEMID_INDEX).toString());
                mostSimilarItemsConf.setInt(MAX_SIMILARITIES_PER_ITEM, maxSimilarItemsPerItem);
                boolean succeeded = mostSimilarItems.waitForCompletion(true);
                if (!succeeded) {
                    return -1;
                }
            }

            return 0;
        }

        public static class MostSimilarItemPairsMapper
                extends Mapper<IntWritable,VectorWritable,Text,DoubleWritable> {

            private OpenIntLongHashMap indexItemIDMap;
            private int maxSimilarItemsPerItem;

            @Override
            protected void setup(Mapper.Context ctx) {
                Configuration conf = ctx.getConfiguration();
                maxSimilarItemsPerItem = conf.getInt(SimilarJob.MAX_SIMILARITIES_PER_ITEM, -1);
                indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(SimilarJob.ITEM_ID_INDEX_PATH_STR), conf);

                Preconditions.checkArgument(maxSimilarItemsPerItem > 0, "maxSimilarItemsPerItem was not correctly set!");
            }

            @Override
            protected void map(IntWritable itemIDIndexWritable, VectorWritable similarityVector, Context ctx)
                    throws IOException, InterruptedException {

                int itemIDIndex = itemIDIndexWritable.get();

//                TopK<RecScorePair> topKMostSimilarItems =
//                        new TopK<RecScorePair>(maxSimilarItemsPerItem, RecScorePair.COMPARE_BY_SIMILARITY);
//
//                Iterator<Vector.Element> similarityVectorIterator = similarityVector.get().nonZeroes().iterator();
//
//                while (similarityVectorIterator.hasNext()) {
//                    Vector.Element element = similarityVectorIterator.next();
//                    topKMostSimilarItems.offer(new RecScorePair(indexItemIDMap.get(element.index()), element.get()));
//                }
//
//                long itemID = indexItemIDMap.get(itemIDIndex);
//                for (RecScorePair similarItem : topKMostSimilarItems.retrieve()) {
//                    long otherItemID = similarItem.getRecId();
//                    ctx.write(new Text(itemID +"\t" + otherItemID), new DoubleWritable(similarItem.getScore()));
//                }
            }
        }
}

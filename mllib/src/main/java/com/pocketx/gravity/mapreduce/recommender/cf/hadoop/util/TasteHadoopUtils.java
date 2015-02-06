package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;

import java.util.regex.Pattern;

public final class TasteHadoopUtils {

	/** Standard delimiter of textual preference data */
	private static final Pattern PREFERENCE_TOKEN_DELIMITER = Pattern.compile("[\t,]");

	private TasteHadoopUtils() {}

	/**
	 * Splits a preference data line into string tokens
   	*/
	public static String[] splitPrefTokens(CharSequence line) {
		return PREFERENCE_TOKEN_DELIMITER.split(line);
	}

	/**
	 * Maps a long to an int
	 */
	public static int idToIndex(long id) {
		return 0x7FFFFFFF & Longs.hashCode(id);
	}

	/**
	 * Reads a binary mapping file
	 */
	public static OpenIntLongHashMap readItemIDIndexMap(String itemIDIndexPathStr, Configuration conf) {
		OpenIntLongHashMap indexItemIDMap = new OpenIntLongHashMap();
		Path itemIDIndexPath = new Path(itemIDIndexPathStr);
		for (Pair<VarIntWritable,VarLongWritable> record
				: new SequenceFileDirIterable<VarIntWritable,VarLongWritable>(itemIDIndexPath,
						PathType.LIST,
						PathFilters.partFilter(),
						null,
						true,
						conf)) {
			indexItemIDMap.put(record.getFirst().get(), record.getSecond().get());
		}
		return indexItemIDMap;
	}
}


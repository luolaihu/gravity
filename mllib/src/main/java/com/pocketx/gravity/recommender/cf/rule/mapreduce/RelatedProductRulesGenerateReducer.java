package com.pocketx.gravity.recommender.cf.rule.mapreduce;


public class RelatedProductRulesGenerateReducer extends
		RelatedRulesGenerateReducer {
	@Override
	protected Double calculateScore(long support, double IDF, double loyalty) {
		return Math.log(1 + (double) support * IDF * loyalty);
	}
}

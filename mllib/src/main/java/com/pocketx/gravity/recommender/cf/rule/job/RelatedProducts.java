package com.pocketx.gravity.recommender.cf.rule.job;

import com.pocketx.gravity.recommender.cf.rule.mapreduce.RelatedProductRulesGenerateReducer;
import com.pocketx.gravity.recommender.cf.rule.model.AssociationRuleJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;

/**
 * @author zhongxiaodong
 * @version created 2013-7-8
 */
public class RelatedProducts {
	private final static int minSupport = 1;
	private final static double minConfidence = 0;
	private final static double minLoyalty = 0;

	public static void main(String[] args) throws IOException {

        String serialProds = "/user/pms/recsys/algorithm/datasource/serial";//数据未用
        String transPath = "/user/pms/recsys/algorithm/schedule/temporary/transaction/product/2015-02-05";
        String freQuenPath = "/user/pms/workspace/luolaihu/gravity/freq";
        String disPath = "/user/pms/workspace/luolaihu/gravity/dispath";
        String relatePath = "/user/pms/workspace/luolaihu/gravity/relate";
        RelatedProducts relatedProducts = new RelatedProducts();

		Configuration conf = new Configuration();
		// configure
        conf.setInt("mapred.reduce.tasks", 20);
        conf.set("mapred.child.java.opts","-Xmx1024m");
        conf.set("mapred.job.queue.name","pms");

		// 2. generate related products
		System.out.println(new Date().toString()
				+ "INFO: start generate related product");
		try {
			relatedProducts.generateRelatedProducts(transPath,freQuenPath, conf);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		// 3. calculate related product rules
		System.out.println(new Date().toString()
				+ "INFO: start calculate related product rules");
		try {
			relatedProducts.calculateRelatedRules(freQuenPath,serialProds,disPath,relatePath,conf);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		System.out.println(new Date().toString()
				+ "INFO: generate related products finish!");
	}

	/**
	 * generate related products using association rule
	 * @throws Exception
	 */
	public void generateRelatedProducts(String productTransPath,
			String productFreqPatternPath, Configuration conf) throws Exception {
		AssociationRuleJob binRelationAr = new AssociationRuleJob();

		// configure
		binRelationAr.setMinSupport(minSupport);
		conf.set("mapred.reduce.tasks", "16");

		binRelationAr.generateRelatedItems(productTransPath,
				productFreqPatternPath, conf);
	}

	/**
	 * calculate related product rules after generate related products
	 * 
	 * @param productFreqPatternPath
	 * @param serialProdsPath
	 * @param itemDistributePath
	 * @param relatedProductTPath
	 * @throws Exception
	 */
	public void calculateRelatedRules(String productFreqPatternPath,
			String serialProdsPath, String itemDistributePath,
			String relatedProductTPath,Configuration conf) throws Exception {
		// configure
        BinaryRelationJob binaryRelationJob = new BinaryRelationJob();
        binaryRelationJob.setMinSupport(minSupport);
        binaryRelationJob.setMinConfidence(minConfidence);
        binaryRelationJob.setMinLoyalty(minLoyalty);
        binaryRelationJob.setRelatedRulesReducer(RelatedProductRulesGenerateReducer.class);

        String[] args = new String[6];
        // build args
        args[0] = "--input";
        args[1] = productFreqPatternPath;
        args[2] = "--output";
        args[3] = relatedProductTPath;
        args[4] = "--itemdistribute";
        args[5] = itemDistributePath;

		// generate rules
        ToolRunner.run(conf,binaryRelationJob,args);
    }
}

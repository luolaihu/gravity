package com.pocketx.gravity.recommender.cf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by luolaihu1 on 2015/2/4.
 */

public class PiplineDriver {

    public int trainDay;
    public String resultDir;
    public String tempPath;
    public String booleanData;
    public String maxSimilarPerItem;
    public String maxPrefsPerUser;
    public String minPrefsPerUser;
    public String similarClassName;
    public String seedFilePath;

    public static void main(String[] args){
        PiplineDriver piplineDriver = new PiplineDriver();
        piplineDriver.booleanData = "true";
        piplineDriver.maxPrefsPerUser = "1000";
        piplineDriver.minPrefsPerUser = "1";
        piplineDriver.maxSimilarPerItem = "200";
        piplineDriver.trainDay = 180;
        piplineDriver.similarClassName = " org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity";
        if(args.length == 3) {
            piplineDriver.seedFilePath = args[0];
            piplineDriver.resultDir = args[1];
            piplineDriver.tempPath = args[2];
        }
        piplineDriver.startPipline();
    }

    public void startPipline(){
        Configuration conf = new Configuration();
        conf.setInt("mapred.reduce.tasks", 20);
        conf.set("mapred.child.java.opts","-Xmx1024m");
        conf.set("mapred.job.queue.name","pms");
        conf.set("pool_name","yihaodian/htms-imp");

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String version = df.format(new Date(new Date().getTime() - 24*1000*3600));
        String resultData = resultDir + File.separator + version;

        StringBuffer input = new StringBuffer(seedFilePath);
        File seedFile = new File(seedFilePath);

        try {
            Date date = df.parse(seedFile.getName());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            FileSystem fs = FileSystem.get(conf);
            for(int i = 1; i < trainDay; i++){
                calendar.add(Calendar.DAY_OF_YEAR,-1);
                String filePath = seedFile.getParent() + File.separator + df.format(calendar.getTime());
                if(fs.exists(new Path(filePath))){
                    input.append(","+filePath);
                }
            }

            if(fs.exists(new Path(tempPath))){
                fs.delete(new Path(tempPath),true);
            }

            if(fs.exists(new Path(resultData))){
                fs.delete(new Path(resultData),true);
            }

            String arg[] = {"--input",input.toString(),"--output",resultData,"--tempDir",tempPath,"--booleanData",booleanData,"--maxSimilaritiesPerItem",
                    maxSimilarPerItem,"--maxPrefs",maxPrefsPerUser,"--minPrefsPerUser",minPrefsPerUser,"--similarityClassname",similarClassName};
            int status = ToolRunner.run(conf,new ItemSimilarityJob(),arg);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

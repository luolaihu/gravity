package com.pocketx.gravity.mapreduce.recommender.cf.hadoop.util;

import java.io.Serializable;
import java.util.Comparator;

public class RecScorePair {

  public static final Comparator<RecScorePair> COMPARE_BY_SIMILARITY = new RecScorePairComparator(false);
  public static final Comparator<RecScorePair> COMPARE_BY_SIMILARITY_DESC = new RecScorePairComparator(true);
  private final long recId;
  private final double score;

  public RecScorePair(long recId, double score) {
    this.recId = recId;
    this.score = score;
  }

  public long getRecId() {
	  return recId;
  }

  public double getScore() {
	return score;
  }
  
  public String toString(){
	  return recId+":"+score;
  }
  
  static class RecScorePairComparator implements Comparator<RecScorePair>, Serializable{
	  
	  boolean desc;
	  
	  public RecScorePairComparator(boolean descOrNot){
		  this.desc = descOrNot;
	  }
	  
	  @Override
	  public int compare(RecScorePair s1, RecScorePair s2) {
		  if(desc)
			  return s1.score == s2.score ? 0 : s1.score < s2.score ? 1 : -1;
		  else
			  return s1.score == s2.score ? 0 : s1.score < s2.score ? -1 : 1;
	  }
  }
  public static void main(String args[]){
	  System.out.println(Runtime.getRuntime().maxMemory());
  }
}

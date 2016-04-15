package com.cg.mapreduce.fpgrowth.standalone;

public class ItemsRecord {
	  public int count;
	  public boolean isUsed;
            
      public  ItemsRecord(int c,boolean b) {
    	  count=c;
    	  isUsed =b;
	  } 
      public void used(){
    	  isUsed = true;
      }
}

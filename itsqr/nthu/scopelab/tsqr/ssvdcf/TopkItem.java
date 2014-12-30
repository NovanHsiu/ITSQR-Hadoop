package nthu.scopelab.tsqr.ssvdcf;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.io.Serializable;


public class TopkItem implements Serializable{
	private int numrec;
	private int curnumitem;
	private Item[] item;
	
	public TopkItem(int numrec)
	{
	 this.numrec = numrec;
	 this.item = new Item[numrec+1];
	 for(int i=0;i<item.length;i++)
	  item[i] = new Item();
	 this.curnumitem = 0;
	}
	
	public TopkItem(int numrec,int numitem)
	{
	 this.numrec = numrec;
	 this.item = new Item[numrec+1];
	 for(int i=0;i<item.length;i++)
	  item[i] = new Item();
	 this.curnumitem = numitem;
	}
	
	public void offer(long id,double value)
	{
	 if(curnumitem<numrec)
	 {
	  item[curnumitem].set(id,value);
	  curnumitem++;
	 }
	 else
	 {
	  item[numrec].set(id,value);
	  //sorting
	  Arrays.sort(item,new Comparator<Item>(){
	  @Override
	  public int compare(Item o1, Item o2)
	  {
	   if(o1.getValue()>o2.getValue())
		return -1;
	   else if(o1.getValue()<o2.getValue())
	    return 1;
	   else
	    return 0;
	  }
	  }); //Arrays.sort	  
	 }
	}
		
	public int numItem()
	{
	 return curnumitem;
	}
	
	public int numRecommendation()
	{
	 return numrec;
	}
	
	public void set(int i,Long id,Double value)
	{
	 this.item[i].set(id,value);
	}
	
	public Long getID(int i)
	{
	 return this.item[i].getID();
	}
	
	public Double getValue(int i)
	{
	 return this.item[i].getValue();
	}
	
	@Override
	public String toString()
	{
	 String text = "";
	 for(int i=0;i<curnumitem;i++)
	  text = text + Long.toString(item[i].getID())+":" + Double.toString(item[i].getValue())+",";
	 return text;
	}
	
	public static class Item{
	 private long id;
	 private double val;
	 public Item()
	 {
	  id = 0;
	  val = 0;
	 }
	 
	 public Item(long id, double val)
	 {
	  this.id = id;
	  this.val = val;
	 }
	 	 
	 public long getID()
	 {
	  return id;
	 }
	 
	 public double getValue()
	 {
	  return val;
	 }
	 
	 public void set(long id, double val)
	 {
	  this.id = id;
	  this.val = val;
	 }
	 	 
	}
}
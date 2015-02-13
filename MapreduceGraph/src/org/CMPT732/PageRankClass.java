package org.CMPT732;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;


public class PageRankClass {
	 
	    private double rank;
	    private ArrayList<Integer> neighbors = new ArrayList<Integer>();
	 
	    public PageRankClass() {
	        set(1, new ArrayList<Integer>());
	    }
	 
	    public PageRankClass(int rank, ArrayList<Integer> neighbors) {
	        set(rank, neighbors);
	    }
	 
	    public PageRankClass(String first) {
	        String[] input = first.split(",");
	        ArrayList<Integer> neighbors = new ArrayList<Integer>();
	        
	        if (input[1] != null && !input[1].equals("null") && !input[1].equals("")){
	        	String[] neighborstring = input[1].trim().split("\\s+");
	        	//System.out.println(input[1].trim() + neighborstring.length);
		        for(int n = 0; n < neighborstring.length; n++) {
		        	//System.out.println(n);
		           neighbors.add(Integer.parseInt(neighborstring[n]));
			         }
		        }
		    else{
		        neighbors=null;
		        }
	    	set(Integer.parseInt(input[0]), neighbors);
	    }
	    
	    
	    public PageRankClass get(){
	    	
	    	return this; 
	    }
	 
	   
	    public ArrayList<Integer> getneighbors() {
	        return neighbors;
	    }
	    public double getrank(){
	    	
	    	return rank;
	    	
	    }
	    public void set(double rank, ArrayList<Integer> neighbors) {
	        this.rank = rank;
	        this.neighbors = neighbors;
	        //this.distance = distance;
	      /*  if(neighbors!=null && neighbors.size()!=0){
		    	neighbors_str = neighbors.toString().replaceAll("[^0-9]", " ");
		    	}
	        else{neighbors_str="null";}*/
	    }
	   
	 
	  /*  @Override
	    public void readFields(DataInput in) throws IOException {
	        //PageClass p = new PageClass();
	        distance = in.readInt();
	        int path_size = in.readInt();
	        
	        for(int i=0; i<path_size;i++){
	        	path.add(in.readInt());
	        	
	        }
	        int neighbors_size = in.readInt();
	        for(int i=0; i<neighbors_size;i++){
	        	neighbors.add(in.readInt());
	        	
	        }
	        
	    }
	 
	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeInt(distance);
	        if(path != null && path.size() !=0){
		        out.writeInt(path.size());
		        for(int i=0;i<path.size();i++){
		        	out.writeInt(path.get(i));
		        }
	        }
	        else{
	        	out.writeInt(0);
	        	}
	        if(neighbors != null && neighbors.size()!=0){
		        out.writeInt(neighbors.size());
		        for(int i=0;i<neighbors.size();i++){
		        	out.writeInt(neighbors.get(i));
		        }
	        }
	        else{
	        	out.writeInt(0);
	        }
	    }
	 
	    @Override*/
	    public String toString() {
	    	//String path_str =null;
	    	String neighbors_str = null; 
	    	
	    	if(neighbors!=null && neighbors.size()!=0){
	    	neighbors_str = neighbors.toString().replaceAll("[^0-9]", " ");
	    	}
	    	else{neighbors_str = "null";}
	        return Double.toString(rank) + "," + neighbors_str;
	    }
/*	 
	  	@Override
	  public int compareTo( Object o) {
	    	PageClass tp = (PageClass) o;
	        int cmp = first.compareTo(tp.first);
	 
	        if (cmp != 0) {
	            return cmp;
	        }
	 
	        return second.compareTo(tp.second);
	    }
	
	    @Override
	    public int hashCode(){
	        return first.hashCode()*163 + second.hashCode();
	    }
	 
	    @Override
	    public boolean equals(Object o)
	    {
	        if(o instanceof PageClass)
	        {
	        	PageClass tp = (PageClass) o;
	            return first.equals(tp.first) && second.equals(tp.second);
	        }
	        return false;
	    }
 */	 
	}



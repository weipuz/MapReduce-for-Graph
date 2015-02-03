package org.CMPT732;

import java.util.*;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

public class PageClass implements Writable{
	 
	    private int distance;
	    private ArrayList<Integer> path = new ArrayList<Integer>();
	    private ArrayList<Integer> neighbors = new ArrayList<Integer>();
	 
	   /* public PageClass(Text first, Text second) {
	        set(first, second);
	    }
	 */
	    public PageClass(int distance, ArrayList<Integer> path, ArrayList<Integer> neighbors) {
	        set(distance, path, neighbors);
	    }
	 
	    public PageClass(String first) {
	        String[] input = first.split(",");
	        ArrayList<Integer> path = new ArrayList<Integer>();
	        ArrayList<Integer> neighbors = new ArrayList<Integer>();
	        
	        String[] pathstring = input[1].split(" ");
	        String[] neighborstring = input[2].split(" ");

	        if (pathstring != null){
		        for(int n = 0; n < pathstring.length; n++) {
		           path.add(Integer.parseInt(pathstring[n]));
		         }
	        }
	        else{
	        	path.add(null);
	        }
	        if (neighborstring != null){
		        for(int n = 0; n < neighborstring.length; n++) {
		           neighbors.add(Integer.parseInt(neighborstring[n]));
			         }
		        }
		    else{
		        neighbors.add(null);
		        }
	    	set(Integer.parseInt(input[0]), path,neighbors);
	    }
	 
	    public ArrayList<Integer> getPath() {
	        return path;
	    }
	 
	    public ArrayList<Integer> getneighbors() {
	        return neighbors;
	    }
	    public int getdistance(){
	    	
	    	return distance;
	    	
	    }
	    public void set(int distance, ArrayList<Integer> path, ArrayList<Integer> neighbors) {
	        this.path = path;
	        this.neighbors = neighbors;
	        this.distance = distance;
	    }
	 
	    @Override
	    public void readFields(DataInput in) throws IOException {
	        
	    }
	 
	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeInt(distance);
	        
	        out.writeInt(path.size());
	        for(int i=0;i<path.size();i++){
	        	out.writeInt(path.get(i));
	        }
	        
	        out.writeInt(neighbors.size());
	        for(int i=0;i<neighbors.size();i++){
	        	out.writeInt(neighbors.get(i));
	        }
	    }
/*	 
	    @Override
	    public String toString() {
	        return first + " " + second;
	    }
	 
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



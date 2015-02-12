package org.CMPT732;

import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

public class PageClass implements Writable{
	 
	    private int distance;
	    //private String path_str = null;
	    //private String neighbors_str = null;
	    private ArrayList<Integer> path = new ArrayList<Integer>();
	    private ArrayList<Integer> neighbors = new ArrayList<Integer>();
	 
	    public PageClass() {
	        set(Integer.MAX_VALUE,  new ArrayList<Integer>(),  new ArrayList<Integer>());
	    }
	 
	    public PageClass(int distance, ArrayList<Integer> path, ArrayList<Integer> neighbors) {
	        set(distance, path, neighbors);
	    }
	 
	    public PageClass(String first) {
	        String[] input = first.split(",");
	        ArrayList<Integer> path = new ArrayList<Integer>();
	        ArrayList<Integer> neighbors = new ArrayList<Integer>();
	        
	        
	        

	        if (input[1] != null && !input[1].equals("null") && !input[1].equals("")){
	        	String[] pathstring = input[1].trim().split("\\s+");
	        	//System.out.println(input[1]);
		        for(int n = 0; n < pathstring.length; n++) {
		           path.add(Integer.parseInt(pathstring[n]));
		         }
	        }
	        else{
	        	path=null;
	        }
	        
	        if (input[2] != null && !input[2].equals("null") && !input[2].equals("")){
	        	String[] neighborstring = input[2].trim().split("\\s+");
	        	//System.out.println(input[2].trim() + neighborstring.length);
		        for(int n = 0; n < neighborstring.length; n++) {
		        	//System.out.println(n);
		           neighbors.add(Integer.parseInt(neighborstring[n]));
			         }
		        }
		    else{
		        neighbors=null;
		        }
	    	set(Integer.parseInt(input[0]), path,neighbors);
	    	//this.path_str = input[1];
	    	//this.neighbors_str = input[2];
	    }
	    
	    
	    public PageClass get(){
	    	
	    	return this; 
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
	      /*  if(neighbors!=null && neighbors.size()!=0){
		    	neighbors_str = neighbors.toString().replaceAll("[^0-9]", " ");
		    	}
	        else{neighbors_str="null";}*/
	    }
	    public void set(int distance, ArrayList<Integer> path) {
	        this.path = path;
	        //this.neighbors = null;
	        this.distance = distance;
	    }
	 
	    @Override
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
	 
	    @Override
	    public String toString() {
	    	String path_str =null;
	    	String neighbors_str = null; 
	    	
	    	
	    	if (path!= null && path.size() !=0){
	    	path_str = path.toString().replaceAll("[^0-9]", " ");
	    	}
	    	else{path_str = "null";}
	    	if(neighbors!=null && neighbors.size()!=0){
	    	neighbors_str = neighbors.toString().replaceAll("[^0-9]", " ");
	    	}
	    	else{neighbors_str = "null";}
	        return Integer.toString(distance) + "," + path_str +"," + neighbors_str;
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



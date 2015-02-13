package org.CMPT732;

import java.io.*;
import java.util.*;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankMain {
	
	
static private final Path TMP_DIR = new Path(PageRankMain.class.getSimpleName() + "_TMP_");
	
public static class RankMap extends Mapper<LongWritable, Text, IntWritable, Text> {
    	
    	private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            String[] inArray = in.split("\t");
            int p = Integer.parseInt(inArray[0]);
            //String page = inArray[1]+ inArray[2]+inArray[3];
            PageClass page = new PageClass(inArray[1]);
            
            context.write(new IntWritable(p) , new Text(page.toString()));
            System.out.println("mapper output:  "+ Integer.toString(p) + page.toString());
            if(page.getdistance()!= Integer.MAX_VALUE){
            	ArrayList<Integer> neighbors = page.getneighbors();
            	for(int i=0;i<neighbors.size();i++){
            		int neighbour_id = neighbors.get(i);
            		int neighbour_distance = page.getdistance() +1;
            		ArrayList<Integer> neighbour_path = new ArrayList<Integer>();
            		if(page.getPath() != null && !page.getPath().equals("null") && !page.getPath().equals("")){
            			neighbour_path = page.getPath();
            			neighbour_path.add(p);
            			 
            			
            		}
            		else{
            			neighbour_path.add(p);
            		}
            		
            		//ArrayList<Integer> neighbour_neighbour = new ArrayList<Integer>();
            		PageClass new_page = new PageClass(neighbour_distance, neighbour_path, null);
            		context.write(new IntWritable(neighbour_id) , new Text(new_page.toString()));
            		System.out.println("mapper output:  "+ Integer.toString(neighbour_id) + new_page.toString());
       
            		
            		
            	}
            }
            
            	
            	
            
            
        }
    }
	
	public static class RankReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
            // Write me
        	
        	int count=0;
        	int distance = Integer.MAX_VALUE;
        	ArrayList<Integer> path = new ArrayList<Integer>();
        	//ArrayList<Integer> neighbor = new ArrayList<Integer>();
        	PageClass page = new PageClass();
        	Iterator<Text> iter = values.iterator();
        	while (iter.hasNext())
        	{
        		
        		PageClass neighbors = new PageClass(iter.next().toString());
        		System.out.println("reducer input:  "+ key.toString() + neighbors.toString());
        			//System.out.println(neighbors.toString());
        		count++;	
        		if(neighbors.getneighbors()!=null && neighbors.getneighbors().size()!=0){
        			page = neighbors; //
        		}
        		else if(neighbors.getdistance() < distance){
        			distance = neighbors.getdistance();
        			path = neighbors.getPath();
        		}
        	}
        	//if(count!=1){
        	//System.out.println(Integer.toString(count));
        	//}
        	page.set(distance, path);
        	
        	context.write(key, new Text(page.toString()));
        	
        }
    }
	
	
	

	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
      //mapreduce
		long starttime = System.currentTimeMillis();
		  Job job = Job.getInstance(new Configuration());
		  job.setJarByClass(PageRankMain.class);
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(Text.class);
		  job.setMapperClass(RankMap.class);
		  job.setReducerClass(RankReduce.class);
		  //job.setNumReduceTasks(0);
		  //job.getConfiguration().set("mapreduce.textoutputformat.separator", ":");
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  
		  //job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size);
        
        
        
        
		
        
        int count = 0;
		String source = args[0];
 
		File fin = new File(source);
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
 
		
		boolean flag = true;
		final FileSystem fs = FileSystem.get(job.getConfiguration());
        //FileContext fc = FileContext.getFileContext();
        if (fs.exists(TMP_DIR)) {
        	flag = false;//don't need to read the original file first; 
        	
        }
        //if (!fs.mkdirs(inDir)) {
       //     throw new IOException("Cannot create input directory " + inDir);
       // }
        
        Path Mapfile = new Path(inDir, "input");
        
      /*  if (flag){
        
	        OutputStreamWriter fstream = new OutputStreamWriter(fs.create(Mapfile,true));
			BufferedWriter out = new BufferedWriter(fstream);
	        
			String aLine = null;
			while ((aLine = in.readLine()) != null) {
				//Process each line and add output to Dest.txt fileha
				String[] alines = aLine.split(":");
				//alines[1].split[" "];
				if(alines[0].equals("2")){    //set the distance of the source node to 0; 
					
				out.write(alines[0]+"\t"+ 0 +",null,"+alines[1]);
				out.newLine();	
					
				}
				else{
				out.write(alines[0]+"\t"+ MAX +",null,"+alines[1]);
				out.newLine();
				}
			}
			
			// do not forget to close the buffer reader
			in.close();
	 
			// close buffer writer
			out.close();  
		
        }
      
        
        
        */
        
        
        //SequenceFile.Writer sqwr = SequenceFile.createWriter(job.getConfiguration(), SequenceFile.Writer.file(outDir), SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(LongWritable.class));
        
        
        
        
        
        
        FileInputFormat.setInputPaths(job, Mapfile);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println(time);
		

	}
}

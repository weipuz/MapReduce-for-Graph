package org.CMPT732;
import java.io.*;
import java.util.*;

//import org.CMPT732A1.WordCount;
//import org.CMPT732A1.WordCount.Map;
//import org.CMPT732A1.WordCount.Reduce;
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


public class GraphMain {

	static private final Path TMP_DIR = new Path(GraphMain.class.getSimpleName() + "_TMP_");
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
    	
    	private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            String[] inArray = in.split("\t");
            int p = Integer.parseInt(inArray[0]);
            //String page = inArray[1]+ inArray[2]+inArray[3];
            PageClass page = new PageClass(inArray[1]);
            
            context.write(new IntWritable(p) , new Text(page.toString()));
            //System.out.println("mapper output:  "+ Integer.toString(p) + page.toString());
            if(page.getdistance()!= Integer.MAX_VALUE){
            	ArrayList<Integer> neighbors = page.getneighbors();
            	
            
            	
            	
            if(neighbors != null && !neighbors.equals("null") && !neighbors.equals("")){	
            	for(int i=0;i<neighbors.size();i++){
            		int neighbour_id = neighbors.get(i);
            		int neighbour_distance = page.getdistance() +1;
            		
            		ArrayList<Integer> neighbour_path = new ArrayList<Integer>();
            		if(page.getPath() != null && !page.getPath().equals("null") && !page.getPath().equals("")){
                		neighbour_path = new ArrayList<Integer>(page.getPath());// bug fixed: used shallow copy to avoid direct reference of the old object;
                	}	
            		neighbour_path.add(p);
            		//ArrayList<Integer> neighbour_neighbour = new ArrayList<Integer>();
            		PageClass new_page = new PageClass(neighbour_distance, neighbour_path, null);
            		context.write(new IntWritable(neighbour_id) , new Text(new_page.toString()));
            		//System.out.println("mapper output:  key: "+ Integer.toString(neighbour_id) + " value: " + new_page.toString());
       
            		
            		
            	}
            	}
            }
            
            	
            	
            
            
        }
    }
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, PageClass> {
		
		private int endpage;
		private boolean found; 
		
		public void setup(Context context) throws IOException,
		InterruptedException {
			Configuration conf = context.getConfiguration();
			endpage = conf.getInt("endpage",1);
			//System.out.println("endpage in reducer is :"+ endpage);
			
		}
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
            // Write me
        	Configuration conf = context.getConfiguration();
        	
        	int count=0;
        	int distance = Integer.MAX_VALUE;
        	ArrayList<Integer> path = new ArrayList<Integer>();
        	//ArrayList<Integer> neighbor = new ArrayList<Integer>();
        	PageClass page = new PageClass();
        	Iterator<Text> iter = values.iterator();
        	while (iter.hasNext())
        	{
        		
        		PageClass neighbors = new PageClass(iter.next().toString());
        		//System.out.println("reducer input:  key: "+ key.toString() + " value: "+ neighbors.toString());
        			//System.out.println(neighbors.toString());
        		count++;	
        		if(neighbors.getneighbors()!=null && neighbors.getneighbors().size()!=0){
        			page = neighbors; //
        		}
        		//else if(neighbors.getdistance() < distance){
        		if(neighbors.getdistance() < distance){
        			distance = neighbors.getdistance();
        			path = neighbors.getPath();
        		}
        	}
        	if(key.get() == endpage && distance != Integer.MAX_VALUE){
        	//if(key.get() == endpage){
        		context.getCounter("Found","Result").increment(endpage);
        		context.getCounter("Found","Result").setDisplayName(path.toString());
        	    //System.out.println("key: "+ key.toString() + "distance: " + Integer.toString(distance));
        		//conf.setBoolean("found", true);
        		//conf.set("shortestpath", path.toString()); //not working; conf cannot pass out to the main;
        	    
        	}
        	page.set(distance, path);
        	context.write(key, page);
        	
        }
        
        
       
        
    }
	
	
	

	public static void writeFile(int startPage, String source, Path inputPath,Job job) throws Exception {
		final int MAX = Integer.MAX_VALUE ;
		File fin = new File(source);
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
		
		final FileSystem fs = FileSystem.get(job.getConfiguration());
		OutputStreamWriter fstream = new OutputStreamWriter(fs.create(inputPath,true));
		BufferedWriter out = new BufferedWriter(fstream);
        
		String aLine = null;
		while ((aLine = in.readLine()) != null) {
			//Process each line and add output to Dest.txt fileha
			String[] alines = aLine.split(":");
			//alines[1].split[" "];
			if(Integer.parseInt(alines[0])==startPage){    //set the distance of the source node to 0; 
				
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
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//-----------------------------------
		boolean cont=true; // flag to decide when to abort while loop
        int ct=0; // decide if this is the first time run or not, first time run reads from original page file, other run reads from MapReduce output file
        int numLoop = 0; // given # of loops
        //JobConf conf = null;
        FileSystem fs;
        Job job = null;
       
        final int MAX = Integer.MAX_VALUE ;
		String source = args[0];
        int startPage = Integer.parseInt(args[1]);
        int endPage = Integer.parseInt(args[2]);

        //System.out.println("Start Page is " + startPage);
        //System.out.println("End Page is " + endPage);
       
        try{
                while(cont) 
                {
                	    Configuration conf = new Configuration();
                	    conf.setInt("endpage", endPage);
                	    conf.setBoolean("found", false);
                		job = Job.getInstance(conf);
                		job.setJarByClass(GraphMain.class);
                        //if(job==null)
                                //return -1;
                       
                        if(ct==0){
                        	Path Mapfile = new Path(TMP_DIR + "/input");
                        	//FileInputFormat.setInputPaths(job, Mapfile);
                        	FileInputFormat.setInputPaths(job, Mapfile);
                        	writeFile(startPage,source,Mapfile,job);
                        	
                        }	
                                //job.(cls);(conf, new Path(TMP_DIR + "/input"));
                        else
                            FileInputFormat.setInputPaths(job, new Path(TMP_DIR + "/output/o"+ct));
                       
                        if(ct>1)
                        {
                                fs = FileSystem.get(job.getConfiguration());
                                fs.delete(new Path(TMP_DIR + "/output/o"+(ct-1)), true);
                        }
                       
                        FileOutputFormat.setOutputPath(job, new Path(TMP_DIR + "/output/o"+(ct+1)));
                       
                        job.setInputFormatClass(TextInputFormat.class);
                        job.setMapperClass(Map.class);
                        job.setMapOutputKeyClass(IntWritable.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setReducerClass(Reduce.class);
                        job.setOutputKeyClass(IntWritable.class);
                        job.setOutputValueClass(PageClass.class);
                        job.setOutputFormatClass(TextOutputFormat.class);
                       
                        
                        job.waitForCompletion(true);
                        
                        long foundpage = job.getCounters().findCounter("Found","Result").getValue();
                        String shortestpath = job.getCounters().findCounter("Found","Result").getDisplayName();
                        //conf = job.getConfiguration();       
                       // boolean found = conf.getBoolean("found", false);
                       // System.out.println(" found: " + found);
                       // String shortestpath = conf.get("shortpath");
                       // System.out.println("shortest path found: " + shortestpath);
                        if((int)foundpage == endPage) //reach the result!!!!!!!
                        {
                                cont = false;
                               
                                System.out.println("shortest path found: " + shortestpath);
                                if(ct>1){
                                        fs = FileSystem.get(job.getConfiguration());
                                        fs.delete(new Path(TMP_DIR + "/output/o"+(ct)), true);
                                }
                        }
                        if(numLoop >= 5) {
                        	cont = false;
                        	System.out.println("exceed max number of iteration");
                        	if(ct>1){
                                fs = FileSystem.get(job.getConfiguration());
                                fs.delete(new Path(TMP_DIR + "/output/o"+(ct)), true);
                            }
                        }
                        ct++;
                        numLoop++;
                }              
        }
        catch(Exception e)
        {
                fs = FileSystem.get(job.getConfiguration());
                fs.delete(new Path(TMP_DIR + "/output"), true);
        }
	
	}

	}

        
        /*
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
      //mapreduce
		long starttime = System.currentTimeMillis();
		  Job job = Job.getInstance(new Configuration());
		  job.setJarByClass(GraphMain.class);
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(PageClass.class);
		
		  job.setMapperClass(Map.class);
		  job.setReducerClass(Reduce.class);
		  //job.setNumReduceTasks(0);
		  //job.getConfiguration().set("mapreduce.textoutputformat.separator", ":");
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  
		  //job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size);
        
        
        
        
		
        //File dir = new File(".");
        final int MAX = Integer.MAX_VALUE ;
		String source = args[0];
		//String dest = args[1];
 
		File fin = new File(source);
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
 
		
		boolean flag = true;
		final FileSystem fs = FileSystem.get(job.getConfiguration());
        //FileContext fc = FileContext.getFileContext();
        if (fs.exists(TMP_DIR)) {
            //throw new IOException("Temporary directory " + fs.makeQualified(TMP_DIR)
                 //   + " already exists.  Please remove it first.");
        	flag = false;//don't need to read the original file first; 
        	
        }
        //if (!fs.mkdirs(inDir)) {
       //     throw new IOException("Cannot create input directory " + inDir);
       // }
        
        Path Mapfile = new Path(inDir, "input");
        
        if (flag){
        
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
      
        
        
        
        
        
        //SequenceFile.Writer sqwr = SequenceFile.createWriter(job.getConfiguration(), SequenceFile.Writer.file(outDir), SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(LongWritable.class));
        
        
        
        
        
        
        FileInputFormat.setInputPaths(job, Mapfile);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
        long endtime = System.currentTimeMillis();
        long time = endtime - starttime;
        System.out.println(time);
		

	}

}*/

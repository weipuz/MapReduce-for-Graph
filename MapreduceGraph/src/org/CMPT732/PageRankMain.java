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
	static private int PAGES_NUM = 5716808;
	
	public static class RankMap extends Mapper<LongWritable, Text, IntWritable, Text> {
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
				ArrayList<Integer> neighbors = null;
        		double out_page_rank = 0;
        		double this_page_current_rank =0;
        		String in = value.toString();
        		String[] inArray = in.split("\t");
        		int page_id = Integer.parseInt(inArray[0]);
        		PageRankClass page = new PageRankClass(inArray[1]);
                context.write(new IntWritable(page_id) , new Text(page.toString()));
                
                this_page_current_rank = page.getrank();
                
                if(page.getneighbors() != null && !page.getneighbors().equals("null") && !page.getneighbors().equals("")){	
                	neighbors = new ArrayList<Integer>(page.getneighbors());
                	out_page_rank = this_page_current_rank/neighbors.size();
                	for(int i=0; i< neighbors.size(); i++){
                		PageRankClass temp_page = new PageRankClass(out_page_rank,null);
                		context.write(new IntWritable(neighbors.get(i)) , new Text(temp_page.toString()));
                	}
                } 		
            }       
    	}
	
	public static class RankReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {

        	int count=0;
        	double sum = 0;
        	PageRankClass page = new PageRankClass();
        	Iterator<Text> iter = values.iterator();
        	while (iter.hasNext()) {
        		PageRankClass neighbors = new PageRankClass(iter.next().toString());
        		//System.out.println("reducer input:  "+ key.toString() + neighbors.toString());
        		//System.out.println(neighbors.toString());
        		count++;	
        		if(neighbors.getneighbors()!=null && neighbors.getneighbors().size()!=0){
        			page = neighbors; //
        		}
        		else {
        			sum += neighbors.getrank();
        		}
        	}
        	double page_rank = 0.15/PAGES_NUM + 0.85*sum;
        	page.set(page_rank);
        	
        	context.write(key, new Text(page.toString()));
        	
        }
    }
	
	public static void writeFile( String source, Path inputPath,Job job) throws Exception {
		//final int MAX = Integer.MAX_VALUE ;
		File fin = new File(source);
		FileInputStream fis = new FileInputStream(fin);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
		final FileSystem fs = FileSystem.get(job.getConfiguration());
		OutputStreamWriter fstream = new OutputStreamWriter(fs.create(inputPath,true));
		BufferedWriter out = new BufferedWriter(fstream);
        String aLine = null;
		while ((aLine = in.readLine()) != null) {
			//Process each line and add output to output file
			String[] alines = aLine.split(":");
			double rank = ((double)1/PAGES_NUM);
			out.write(alines[0]+"\t"+ rank + ","+alines[1]);
			out.newLine();
			
		}
		
		in.close(); // do not forget to close the buffer reader
		out.close();  // close buffer writer
	}

	public static void main(String[] args) throws Exception {

		boolean cont=true; // flag to decide when to abort while loop
        int ct=0; // decide if this is the first time run or not, first time run reads from original page file, other run reads from MapReduce output file
        int numLoop = 0; // given # of loops
        FileSystem fs;
        Job job = null;
		String source = args[0];
        while(cont) {
        	    Configuration conf = new Configuration();
        		job = Job.getInstance(conf);
        		job.setJarByClass(GraphMain.class);
                if(ct == 0){
                	Path Mapfile = new Path(TMP_DIR + "/input");
                	FileInputFormat.setInputPaths(job, Mapfile);
                	writeFile(source,Mapfile,job);	
                }	      
                else
                    FileInputFormat.setInputPaths(job, new Path(TMP_DIR + "/output/o"+ct));
               
                if(ct > 1){
                    fs = FileSystem.get(job.getConfiguration());
                    fs.delete(new Path(TMP_DIR + "/output/o"+(ct-1)), true);
                }
                FileOutputFormat.setOutputPath(job, new Path(TMP_DIR + "/output/o"+(ct+1)));
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(RankMap.class);
                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(RankReduce.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.waitForCompletion(true);
                if(numLoop >= 4) 
                {
                	cont = false;
                	//System.out.println("exceed max number of iteration");
                	if(ct>1)
                	{
                        fs = FileSystem.get(job.getConfiguration());
                        fs.delete(new Path(TMP_DIR + "/output/o"+(ct)), true);
                    }
                }
                ct++;
                numLoop++;
         }              
               

	}
}

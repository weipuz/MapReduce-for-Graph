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
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, PageClass> {
    	private int tmax_tmp;
    	
    	private Text word = new Text();
    	private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
            String in = value.toString();
            //String newin = in.replaceAll("[^a-zA-Z ]", "");
            //StringTokenizer tokenizer = new StringTokenizer(in);
            String[] inArray = in.split("\t");
            int p = Integer.parseInt(inArray[0]);
            //String page = inArray[1]+ inArray[2]+inArray[3];
            context.write(new IntWritable(p) , new PageClass(inArray[1]));
            	
            	
            
            //context.write(new Text(in), one);
        }
    }
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		
		final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
      //mapreduc
		long starttime = System.currentTimeMillis();
		  Job job = Job.getInstance(new Configuration());
		  job.setJarByClass(GraphMain.class);
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(PageClass.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(PageClass.class);
		
		  job.setMapperClass(Map.class);
		  //job.setReducerClass(Reduce.class);
		  job.setNumReduceTasks(0);
		  job.getConfiguration().set("mapreduce.textoutputformat.separator", ":");
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
				out.write(alines[0]+"\t"+ MAX +",null,"+alines[1]);
				out.newLine();
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

}

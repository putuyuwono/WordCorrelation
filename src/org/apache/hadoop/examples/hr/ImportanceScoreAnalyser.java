package org.apache.hadoop.examples.hr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImportanceScoreAnalyser {
	public static class MapAClass extends Mapper<Object, Text, Text, Text>{
		private Text outKey = new Text();
		private Text outVal = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] temp = value.toString().split("\t");
			if(temp.length == 2){
				outKey.set(temp[0]);
				outVal.set("A" + temp[1]);
				context.write(outKey, outVal);
			}
		}
	}
	
	public static class MapBClass extends Mapper<Object, Text, Text, Text>{
		private Text outVal = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] temp = value.toString().split("\t");
			String key1 = "", key2 = "";
			if(temp.length == 2){
				String[] keys = temp[0].split(";");
				if(keys.length == 2){
					key1 = keys[0];
					key2 = keys[1];
					outVal.set("B" + temp[1]);
					context.write(new Text(key1), outVal);
					context.write(new Text(key2), outVal);
				}
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {				
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double score = 0, freq = 0, adjCorr = 0, tempAdjCorr = 0;
			for(Text val: values){
				if(val.toString().charAt(0) == 'A'){
					try{
						freq = Double.valueOf(val.toString().substring(1));						
					}catch(Exception ex){
						System.out.println(val.toString());
					}
				}else if(val.toString().charAt(0) == 'B'){
					try{
						tempAdjCorr = Double.valueOf(val.toString().substring(1));
						if(tempAdjCorr > adjCorr){
							adjCorr = tempAdjCorr;
						}						
					}catch(Exception ex){
						System.out.println(val.toString());
					}
				}
			}
			score = freq + adjCorr;
			context.write(key, new Text(String.valueOf(score)));			
		}
	}
	
	public static void run(String inputDir1, String inputDir2, String outputDir) throws Exception{
		Configuration conf = new Configuration();
		System.out.println("Running Total Importance Score Analyser ");
		Job job = new Job(conf, "Total Importance Score Analyser");
		job.setJarByClass(ImportanceScoreAnalyser.class);
		job.setReducerClass(ImportanceScoreAnalyser.ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(inputDir1), TextInputFormat.class, ImportanceScoreAnalyser.MapAClass.class);
		MultipleInputs.addInputPath(job, new Path(inputDir2), TextInputFormat.class, ImportanceScoreAnalyser.MapBClass.class);
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean status = job.waitForCompletion(true) ? false : true;
		if(status){
			return;
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length < 1){
			System.out.println("Please supply the input path!");
			return;
		}
		String inpRoot = args[0];
		String outRoot = inpRoot + "_out";
		if(args.length == 2){
			outRoot = args[1];
		}
		double start = System.currentTimeMillis();
		String inpWC = inpRoot + "/WordCount";
		String outWC = outRoot + "/OUT_WC";
		String inpLevCorr = inpRoot + "/LevelBased";
		String outLevCorr = outRoot + "/OUT_LB";
		String outTotCorr = outRoot + "/OUT_TC";

		EntityFinder.run(inpWC, outWC);
		AdjacentCorrelationAnalyser.run(inpLevCorr, outLevCorr);
		ImportanceScoreAnalyser.run(outWC, outLevCorr, outTotCorr);
		double end = System.currentTimeMillis();
		double dur = (end - start) / 60000;
		System.out.println("Processing Time: " + dur + " minutes.");		
	}
	
}

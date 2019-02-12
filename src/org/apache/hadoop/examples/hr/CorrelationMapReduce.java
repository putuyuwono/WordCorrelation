package org.apache.hadoop.examples.hr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CorrelationMapReduce {
	public static class MapAClass extends Mapper<Object, Text, Text, Text>{
		private Text outKey = new Text();
		private Text outVal = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] temp = value.toString().split("\t");
			if(temp.length == 2){
				outKey.set("A" + temp[0]);
				outVal.set(temp[1]);
				context.write(outKey, outVal);
			}
		}
	}
	
	public static class MapBClass extends Mapper<Object, Text, Text, Text>{
		private Text outKey = new Text();
		private Text outVal = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] temp = value.toString().split("\t");
			if(temp.length == 2){
				outKey.set("B" + temp[0]);
				outVal.set(temp[1]);
				context.write(outKey, outVal);
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		Map<String, String> wordFreq = new HashMap<String, String>();
		Map<String, String> pairLevC = new HashMap<String, String>();
		Map<String, Double> wordCorr = new HashMap<String, Double>();
		
		Text outKey = new Text();
		Text outVal = new Text();
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//perform join operation
			analyzeWordCorr();
			for(Map.Entry<String, Double> entry: wordCorr.entrySet()){
				outKey.set(entry.getKey());
				outVal.set(String.valueOf(entry.getValue()));
				context.write(outKey, outVal);
			}
		}
		
		private void analyzeWordCorr(){
			double freq1 = 0, freq2 = 0, corr1 = 0, corr2 = 0, tmpCorr1 = 0, tmpCorr2 = 0, levCorr = 0;
			for(Map.Entry<String, String> entry: pairLevC.entrySet()){
				String[] temp = entry.getKey().split(";");
				if(temp.length == 2){
					String word1 = temp[0];
					String word2 = temp[1];
					String sfre1 = getWordFreq(word1);
					String sfre2 = getWordFreq(word2);
					String slevC = entry.getValue();
					
					try{
						levCorr = Double.valueOf(slevC);
						freq1 = Double.valueOf(sfre1);
						freq2 = Double.valueOf(sfre2);
						
						tmpCorr1 = getWordCorr(word1);
						tmpCorr2 = getWordCorr(word2);
						
						corr1 = ((freq2 - freq1) * levCorr) + freq1;
						corr2 = ((freq1 - freq2) * levCorr) + freq2;
						
						if(corr1 > tmpCorr1){
							wordCorr.put(word1, corr1);
						}
						
						if(corr2 > tmpCorr2){
							wordCorr.put(word2, corr2);
						}						
					}catch(Exception ex){
					}
				}
			}
		}
		
		private String getWordFreq(String word){
			Map.Entry<String, String> entry = findWordFreq(word);
			if(entry != null){
				return entry.getValue();
			}
			return null;
		}
		
		private Map.Entry<String, String> findWordFreq(String word){
			for(Map.Entry<String, String> entry: wordFreq.entrySet()){
				if(entry.getKey().equalsIgnoreCase(word)){
					return entry;
				}
			}
			return null;
		}
		
		private Double getWordCorr(String word){
			Map.Entry<String, Double> entry = findWordCorr(word);
			if(entry != null){
				return entry.getValue();
			}
			return null;
		}
		
		private Map.Entry<String, Double> findWordCorr(String word){
			for(Map.Entry<String, Double> entry: wordCorr.entrySet()){
				if(entry.getKey().equalsIgnoreCase(word)){
					return entry;
				}
			}
			return null;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context){
			String word = key.toString().substring(1);
			for(Text val: values){
				if(key.toString().charAt(0) == 'A'){
					wordFreq.put(word, val.toString());
					wordCorr.put(word, Double.valueOf(val.toString()));
				}else if(key.toString().charAt(0) == 'B'){
					pairLevC.put(word, val.toString());
				}
			}			
		}
	}
	
	public static void run(String inputDir1, String inputDir2, String outputDir) throws Exception{
		Configuration conf = new Configuration();
		System.out.println("Running Total Correlation Analysis");
		Job job = new Job(conf, "Total Correlation Analysis");
		job.setJarByClass(CorrelationMapReduce.class);
		job.setReducerClass(CorrelationMapReduce.ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(inputDir1), TextInputFormat.class, CorrelationMapReduce.MapAClass.class);
		MultipleInputs.addInputPath(job, new Path(inputDir2), TextInputFormat.class, CorrelationMapReduce.MapBClass.class);
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
		CorrelationMapReduce.run(outWC, outLevCorr, outTotCorr);
		double end = System.currentTimeMillis();
		double dur = (end - start) / 60000;
		System.out.println("Processing Time: " + dur);		
	}
	
	public static void oldmain2(String[] args) throws Exception{
		String inpRoot = "/bio_dataset/";
		String outRoot = "/bio_out/";
		if(args.length < 1){
			System.out.println("Please supply the max. iter!");
			return;
		}
		String maxIter = args[0];
		int max = Integer.valueOf(maxIter);
		double start = System.currentTimeMillis();
		for(int i=1; i<= max; i++){
			String name = String.valueOf(i);
			String inpWC = inpRoot + name + "/WordCount";
			String outWC = outRoot + name + "/OUT_WC";
			String inpLevCorr = inpRoot + name + "/LevelBased";
			String outLevCorr = outRoot + name + "/OUT_LB";
			String outTotCorr = outRoot + name + "/OUT_TC";

			EntityFinder.run(inpWC, outWC);
			LevelBasedCorrelationAnalyser.run(inpLevCorr, outLevCorr);
			CorrelationMapReduce.run(outWC, outLevCorr, outTotCorr);
		}
		double end = System.currentTimeMillis();
		double dur = (end - start) / 1000;
		System.out.println("Processing Time: " + dur);		
	}
	
	public static void oldmain() throws Exception {
		Configuration conf = new Configuration();
		// if (args.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		System.out.println("Running Correlation MapReduce");
		Job job = new Job(conf, "Correlation MapReduce");
		job.setJarByClass(CorrelationMapReduce.class);
		job.setReducerClass(CorrelationMapReduce.ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path("/inputCount"), TextInputFormat.class, CorrelationMapReduce.MapAClass.class);
		MultipleInputs.addInputPath(job, new Path("/inputCorr"), TextInputFormat.class, CorrelationMapReduce.MapBClass.class);
		String output = "/corr-output";
		double start = System.currentTimeMillis();
		boolean status = job.waitForCompletion(true) ? false : true;
		double end = System.currentTimeMillis();
		double dur = end - start;
		System.out.println("Processing Time: " + dur);
		System.exit(status == true ? 0 : 1);
	}
}

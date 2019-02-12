package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImportanceScoreAnalyser {
	static String[] stopwords = { "﻿able", "about", "above", "abroad",
		"according", "accordingly", "across", "actually", "adj",
		"after", "afterwards", "again", "against", "ago", "ahead",
		"ain't", "all", "allow", "allows", "almost", "alone",
		"along", "alongside", "already", "also", "although",
		"always", "am", "amid", "amidst", "among", "amongst", "an",
		"and", "another", "any", "anybody", "anyhow", "anyone",
		"anything", "anyway", "anyways", "anywhere", "apart",
		"appear", "appreciate", "appropriate", "are", "aren't",
		"around", "as", "a's", "aside", "ask", "asking",
		"associated", "at", "available", "away", "awfully", "back",
		"backward", "backwards", "be", "became", "because",
		"become", "becomes", "becoming", "been", "before",
		"beforehand", "begin", "behind", "being", "believe",
		"below", "beside", "besides", "best", "better", "between",
		"beyond", "both", "brief", "but", "by", "came", "can",
		"cannot", "cant", "can't", "caption", "cause", "causes",
		"certain", "certainly", "changes", "clearly", "c'mon",
		"co", "co.", "com", "come", "comes", "concerning",
		"consequently", "consider", "considering", "contain",
		"containing", "contains", "corresponding", "could",
		"couldn't", "course", "c's", "currently", "dare",
		"daren't", "definitely", "described", "despite", "did",
		"didn't", "different", "directly", "do", "does", "doesn't",
		"doing", "done", "don't", "down", "downwards", "during",
		"each", "edu", "eg", "eight", "eighty", "either", "else",
		"elsewhere", "end", "ending", "enough", "entirely",
		"especially", "et", "etc", "even", "ever", "evermore",
		"every", "everybody", "everyone", "everything",
		"everywhere", "ex", "exactly", "example", "except",
		"fairly", "far", "farther", "few", "fewer", "fifth",
		"first", "five", "followed", "following", "follows", "for",
		"forever", "former", "formerly", "forth", "forward",
		"found", "four", "from", "further", "furthermore", "get",
		"gets", "getting", "given", "gives", "go", "goes", "going",
		"gone", "got", "gotten", "greetings", "had", "hadn't",
		"half", "happens", "hardly", "has", "hasn't", "have",
		"haven't", "having", "he", "he'd", "he'll", "hello",
		"help", "hence", "her", "here", "hereafter", "hereby",
		"herein", "here's", "hereupon", "hers", "herself", "he's",
		"hi", "him", "himself", "his", "hither", "hopefully",
		"how", "howbeit", "however", "hundred", "i'd", "ie", "if",
		"ignored", "i'll", "i'm", "immediate", "in", "inasmuch",
		"inc", "inc.", "indeed", "indicate", "indicated",
		"indicates", "inner", "inside", "insofar", "instead",
		"into", "inward", "is", "isn't", "it", "it'd", "it'll",
		"its", "it's", "itself", "i've", "just", "k", "keep",
		"keeps", "kept", "know", "known", "knows", "last",
		"lately", "later", "latter", "latterly", "least", "less",
		"lest", "let", "let's", "like", "liked", "likely",
		"likewise", "little", "look", "looking", "looks", "low",
		"lower", "ltd", "made", "mainly", "make", "makes", "many",
		"may", "maybe", "mayn't", "me", "mean", "meantime",
		"meanwhile", "merely", "might", "mightn't", "mine",
		"minus", "miss", "more", "moreover", "most", "mostly",
		"mr", "mrs", "much", "must", "mustn't", "my", "myself",
		"name", "namely", "nd", "near", "nearly", "necessary",
		"need", "needn't", "needs", "neither", "never", "neverf",
		"neverless", "nevertheless", "new", "next", "nine",
		"ninety", "no", "nobody", "non", "none", "nonetheless",
		"noone", "no-one", "nor", "normally", "not", "nothing",
		"notwithstanding", "novel", "now", "nowhere", "obviously",
		"of", "off", "often", "oh", "ok", "okay", "old", "on",
		"once", "one", "ones", "one's", "only", "onto", "opposite",
		"or", "other", "others", "otherwise", "ought", "oughtn't",
		"our", "ours", "ourselves", "out", "outside", "over",
		"overall", "own", "particular", "particularly", "past",
		"per", "perhaps", "placed", "please", "plus", "possible",
		"presumably", "probably", "provided", "provides", "que",
		"quite", "qv", "rather", "rd", "re", "really",
		"reasonably", "recent", "recently", "regarding",
		"regardless", "regards", "relatively", "respectively",
		"right", "round", "said", "same", "saw", "say", "saying",
		"says", "second", "secondly", "see", "seeing", "seem",
		"seemed", "seeming", "seems", "seen", "self", "selves",
		"sensible", "sent", "serious", "seriously", "seven",
		"several", "shall", "shan't", "she", "she'd", "she'll",
		"she's", "should", "shouldn't", "since", "six", "so",
		"some", "somebody", "someday", "somehow", "someone",
		"something", "sometime", "sometimes", "somewhat",
		"somewhere", "soon", "sorry", "specified", "specify",
		"specifying", "still", "sub", "such", "sup", "sure",
		"take", "taken", "taking", "tell", "tends", "th", "than",
		"thank", "thanks", "thanx", "that", "that'll", "thats",
		"that's", "that've", "the", "their", "theirs", "them",
		"themselves", "then", "thence", "there", "thereafter",
		"thereby", "there'd", "therefore", "therein", "there'll",
		"there're", "theres", "there's", "thereupon", "there've",
		"these", "they", "they'd", "they'll", "they're", "they've",
		"thing", "things", "think", "third", "thirty", "this",
		"thorough", "thoroughly", "those", "though", "three",
		"through", "throughout", "thru", "thus", "till", "to",
		"together", "too", "took", "toward", "towards", "tried",
		"tries", "truly", "try", "trying", "t's", "twice", "two",
		"un", "under", "underneath", "undoing", "unfortunately",
		"unless", "unlike", "unlikely", "until", "unto", "up",
		"upon", "upwards", "us", "use", "used", "useful", "uses",
		"using", "usually", "v", "value", "various", "versus",
		"very", "via", "viz", "vs", "want", "wants", "was",
		"wasn't", "way", "we", "we'd", "welcome", "well", "we'll",
		"went", "were", "we're", "weren't", "we've", "what",
		"whatever", "what'll", "what's", "what've", "when",
		"whence", "whenever", "where", "whereafter", "whereas",
		"whereby", "wherein", "where's", "whereupon", "wherever",
		"whether", "which", "whichever", "while", "whilst",
		"whither", "who", "who'd", "whoever", "whole", "who'll",
		"whom", "whomever", "who's", "whose", "why", "will",
		"willing", "wish", "with", "within", "without", "wonder",
		"won't", "would", "wouldn't", "yes", "yet", "you", "you'd",
		"you'll", "your", "you're", "yours", "yourself",
		"yourselves", "you've", "zero", "﻿a", "how's", "i",
		"when's", "why's", "﻿I", "a", "www", "able", "abst",
		"accordance", "act", "added", "adopted", "affected",
		"affecting", "affects", "ah", "announce", "anymore",
		"apparently", "approximately", "aren", "arent", "arise",
		"auth", "b", "beginning", "beginnings", "begins", "biol",
		"briefly", "c", "ca", "couldnt", "d", "date", "due", "e",
		"ed", "effect", "et-al", "f", "ff", "fix", "g", "gave",
		"give", "giving", "h", "hed", "heres", "hes", "hid",
		"home", "id", "im", "immediately", "importance",
		"important", "index", "information", "invention", "itd",
		"j", "keys", "kg", "km", "l", "largely", "lets", "line",
		"'ll", "m", "means", "mg", "million", "ml", "mug", "n",
		"na", "nay", "necessarily", "nos", "noted", "o", "obtain",
		"obtained", "omitted", "ord", "owing", "p", "page",
		"pages", "part", "poorly", "possibly", "potentially", "pp",
		"predominantly", "present", "previously", "primarily",
		"promptly", "proud", "put", "q", "quickly", "r", "ran",
		"readily", "ref", "refs", "related", "research",
		"resulted", "resulting", "results", "run", "s", "sec",
		"section", "shed", "shes", "show", "showed", "shown",
		"showns", "shows", "significant", "significantly",
		"similar", "similarly", "slightly", "somethan",
		"specifically", "state", "states", "stop", "strongly",
		"substantially", "successfully", "sufficiently", "suggest",
		"t", "thered", "thereof", "therere", "thereto", "theyd",
		"theyre", "thou", "thoughh", "thousand", "throug", "til",
		"tip", "ts", "u", "ups", "usefully", "usefulness", "'ve",
		"vol", "vols", "w", "wed", "whats", "wheres", "whim",
		"whod", "whos", "widely", "words", "world", "x", "y",
		"youd", "youre", "z", "amoungst", "amount", "bill",
		"bottom", "call", "computer", "con", "cry", "de",
		"describe", "detail", "eleven", "empty", "fifteen", "fify",
		"fill", "find", "fire", "forty", "front", "full", "hasnt",
		"herse”", "himse”", "interest", "itse”", "mill", "move",
		"myse”", "side", "sincere", "sixty", "system", "ten",
		"thick", "thin", "top", "twelve", "twenty" };
	
	public static class MapAClass extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private Text vals = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {			
			Set<String> stopWordsSet = new LinkedHashSet<String>();
			for (String s : stopwords) {
				stopWordsSet.add(s);
			}
			String inputText = value.toString().toLowerCase()
					.replaceAll("\\W", " ").trim();
			String[] words = inputText.split(" ");
			for (String w : Arrays.asList(words)) {
				if (w.trim().equalsIgnoreCase(""))
					continue;
				if (!stopWordsSet.contains(w)) {
					word.set(w);
					vals.set("B1");
					context.write(word, vals);
				}
			}
		}
	}
	
	public static class MapBClass extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private Text vals = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Set<String> stopWordsSet = new LinkedHashSet<String>();
			Set<String> mainWordSet = new LinkedHashSet<String>();

			for (String s : stopwords) {
				stopWordsSet.add(s);
			}

			// Step-1: Removing Stopwords
			String inputText = value.toString().toLowerCase();
			String[] words = inputText.replaceAll("\\W", " ").split(" ");
			for (String w : Arrays.asList(words)) {
				if (w.trim().equalsIgnoreCase(""))
					continue;
				if (!stopWordsSet.contains(w)) {
					mainWordSet.add(w);
				}
			}

			// Step-2: Construct the pairs and emit output
			List<String> listWords = new ArrayList<String>(mainWordSet);
			java.util.Collections.sort(listWords);
			for (int i = 0; i < listWords.size(); i++) {
				for (int j = i; j < listWords.size(); j++) {
					String word1 = listWords.get(i);
					String word2 = listWords.get(j);
					if (!word1.equalsIgnoreCase(word2)) {
						word.set(word1 + ";" + word2);
						vals.set("B1");
						context.write(word, vals);
					}
				}
			}
		}
	}
	
	public static class CombineClass extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double freq = 0, freqCooccur = 0;
			for(Text val: values){
				if(val.toString().charAt(0) == 'A'){
					try{
						freq += Double.valueOf(val.toString().substring(1));
					}catch(Exception ex){
						System.out.println(val.toString());
					}
				}else if(val.toString().charAt(0) == 'B'){
					try{
						freqCooccur += Double.valueOf(val.toString().substring(1));
					}catch(Exception ex){
						System.out.println(val.toString());
					}
				}
			}

			String[] keys = key.toString().split(";");
			if(keys.length == 2){
				Text key1 = new Text(keys[0]);
				Text key2 = new Text(keys[1]);
				context.write(key1, new Text(String.valueOf("B" + freqCooccur)));
				context.write(key2, new Text(String.valueOf("B" + freqCooccur)));					
			}else{
				context.write(key, new Text(String.valueOf("A" + freq)));				
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		Map<String, Double> wordCorr = new HashMap<String, Double>();
		Text outKey = new Text();
		Text outVal = new Text();
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<String, Double> entry: wordCorr.entrySet()){
				outKey.set(entry.getKey());
				outVal.set(String.valueOf(entry.getValue()));
				context.write(outKey, outVal);
			}
		}
		
		private void put(String key, double score){
			if(wordCorr.containsKey(key)){
				double curScore = wordCorr.get(key);
				double newScore = curScore + score;
				wordCorr.put(key.toString(), newScore);					
			}else{
				wordCorr.put(key, score);	
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context){
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
			put(key.toString(), score);
		}
	}
	
	public static void main(String[] args) throws Exception{
//		if(args.length < 1){
//			System.out.println("Please supply the input path!");
//			return;
//		}
//		String inpRoot = args[0];
		String inpRoot = "/test";
		String outRoot = inpRoot + "_out";
		if(args.length == 2){
			outRoot = args[1];
		}
		double start = System.currentTimeMillis();
		String inpWC = inpRoot + "/WordCount";
		String inpLevCorr = inpRoot + "/LevelBased";

		Configuration conf = new Configuration();
		System.out.println("Running Total Importance Score Analyser v2");
		Job job = new Job(conf, "Total Importance Score Analyser v2");
		job.setJarByClass(ImportanceScoreAnalyser.class);
		job.setCombinerClass(ImportanceScoreAnalyser.CombineClass.class);
		job.setReducerClass(ImportanceScoreAnalyser.ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(inpWC), TextInputFormat.class, ImportanceScoreAnalyser.MapAClass.class);
		MultipleInputs.addInputPath(job, new Path(inpLevCorr), TextInputFormat.class, ImportanceScoreAnalyser.MapBClass.class);
		FileOutputFormat.setOutputPath(job, new Path(outRoot));
		boolean status = job.waitForCompletion(true) ? false : true;
		if(status){
			return;
		}
		
		double end = System.currentTimeMillis();
		double dur = (end - start) / 60000;
		System.out.println("Processing Time: " + dur + " minutes.");		
	}	
}

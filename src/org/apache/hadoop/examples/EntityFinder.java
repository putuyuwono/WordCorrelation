package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EntityFinder {
	public static class EntityFinderMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] stopwords = { "﻿able", "about", "above", "abroad",
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
					context.write(word, one);
				}
			}
		}
	}

	public static class EntityFinderReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void run(String inputDir, String outputDir) throws Exception {
		Configuration conf = new Configuration();
		System.out.println("Running Word Frequency Counter");
		Job job = new Job(conf, "Word Frequency Counter");
		job.setJarByClass(EntityFinder.class);
		job.setMapperClass(EntityFinderMapper.class);
		job.setCombinerClass(EntityFinderReducer.class);
		job.setReducerClass(EntityFinderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		boolean status = job.waitForCompletion(true) ? false : true;
		if(status){
			return;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// if (args.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		System.out.println("Running Top-K Word Count");
		Job job = new Job(conf, "top-k word count");
		job.setJarByClass(EntityFinder.class);
		job.setMapperClass(EntityFinderMapper.class);
		job.setCombinerClass(EntityFinderReducer.class);
		job.setReducerClass(EntityFinderReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		String input = "/sen-corr-input";
		String output = "/inputCount";
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		boolean status = job.waitForCompletion(true) ? false : true;
		System.exit(status == true ? 0 : 1);
	}
}

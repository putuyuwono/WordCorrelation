package backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import backup.SentenceCorrelationAnalyser;

public class LevelBasedCorrelationAnalyser {
	public static final String CORRELATION_ANALYSIS = "CorrelationAnalysis";
	public final static String RECORD_COUNTER_NAME = "Records";

	public static class MapClass extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private final static DoubleWritable one = new DoubleWritable(1);
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

			Set<String> stopWordsSet = new LinkedHashSet<>();
			Set<String> mainWordSet = new LinkedHashSet<>();

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
						context.write(word, one);
					}
				}
			}

			if (!inputText.trim().equalsIgnoreCase("")) {
				context.getCounter(CORRELATION_ANALYSIS, RECORD_COUNTER_NAME).increment(1);
			}
		}
	}

	public static class ReduceClass extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		double unitCount = 0;
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			long numOfRecord = context.getCounter(CORRELATION_ANALYSIS, RECORD_COUNTER_NAME).getValue();
			System.out.println(numOfRecord);
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			Text out = new Text(key.toString() + "|" + numOfRecord);
			context.write(out, result);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Running Level-Based Correlation Analyser");
		Configuration conf = new Configuration();
		// if (args.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		Job job = new Job(conf, "Level-Based Correlation Analyser");
		job.setJarByClass(LevelBasedCorrelationAnalyser.class);
		job.setMapperClass(LevelBasedCorrelationAnalyser.MapClass.class);
		job.setCombinerClass(LevelBasedCorrelationAnalyser.ReduceClass.class);
		job.setReducerClass(LevelBasedCorrelationAnalyser.ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

//		String input = "/doc-corr-input";
		String output = "/doc-corr-output";
		String senCorr = "/sen-corr-input";
		String parCorr = "/par-corr-input";
		String docCorr = "/doc-corr-input";
		Path senPath = new Path(senCorr);
		Path parPath = new Path(parCorr);
		Path docPath = new Path(docCorr);
		MultipleInputs.addInputPath(job, senPath, TextInputFormat.class, LevelBasedCorrelationAnalyser.MapClass.class);
		MultipleInputs.addInputPath(job, parPath, TextInputFormat.class, LevelBasedCorrelationAnalyser.MapClass.class);
		MultipleInputs.addInputPath(job, docPath, TextInputFormat.class, LevelBasedCorrelationAnalyser.MapClass.class);
//		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

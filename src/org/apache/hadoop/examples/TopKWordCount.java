package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKWordCount {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private static class TopKRecord implements Comparable<TopKRecord> {
			public Text key;
			public int sum;

			public TopKRecord(Text key, int sum) {
				this.key = key;
				this.sum = sum;
			}

			@Override
			public boolean equals(Object obj) {
				if (!(obj instanceof TopKRecord))
					return false;

				TopKRecord other = (TopKRecord) obj;
				if (other.sum == this.sum && other.key.equals(this.key))
					return true;
				else
					return false;
			}

			@Override
			public int hashCode() {
				return this.key.hashCode() ^ (int) this.sum;
			}

			@Override
			public int compareTo(TopKRecord other) {
				if (this.sum == other.sum)
					return this.key.compareTo(other.key);

				return (this.sum < other.sum ? -1 : 1);
			}
		}

		private final TreeSet<TopKRecord> heap = new TreeSet<TopKRecord>();
		int k = 10;

		Context target = null;

		private IntWritable result = new IntWritable();
		
		@Override
		protected void cleanup(Context context) {
			for (TopKRecord rec : this.heap) {
				try {
					context.write(rec.key, new IntWritable(rec.sum));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			this.heap.clear();
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			this.heap.add(new TopKRecord(new Text(key), sum));
			if (this.heap.size() > k) {
				TopKRecord removed = this.heap.pollFirst();
				if (removed == null) {
					throw new IllegalStateException();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}
		System.out.println("Running Top-K Word Count");
		Job job = new Job(conf, "top-k word count");
		job.setJarByClass(TopKWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		String input = "/input";
		String output = "/output1";
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

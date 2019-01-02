package twitter;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Follower extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Follower.class);
	private static int triangleCount = 0;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey1 = new Text();
        private Text outvalue1 = new Text();
        private Text outkey2 = new Text();
        private Text outvalue2 = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			boolean isFiltered = MAXFilter(10000, value);
			if(isFiltered) {
				final String[] followerString = value.toString().split(",");
				outkey1.set(followerString[0]);
				outkey2.set(followerString[1]);
				outvalue1.set(value + "," + "from");
				outvalue2.set(value + "," + "to");
				context.write(outkey1, outvalue1);
				context.write(outkey2, outvalue2);
			}
        }

        public boolean MAXFilter(final int maxValue, final Text value) {
			final String[] followerString = value.toString().split(",");
			if (Integer.parseInt(followerString[0]) > maxValue) {
				return false;
			}
			if (Integer.parseInt(followerString[1]) > maxValue) {
				return false;
			}
			return true;
		}
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
			Set<String> toMap = new HashSet<>();
			Set<String> fromMap = new HashSet<>();
			for (final Text val : values) {
				final String[] followerString = val.toString().split(",");
				if (followerString[2].equals("to")) {
					toMap.add(followerString[0]);
				}
				if (followerString[2].equals("from")) {
					fromMap.add(followerString[1]);
				}
			}
			for (String firstEdge: toMap) {
				for (String lastEdge: fromMap) {
					if (firstEdge.equals(lastEdge)) {
						continue;
					}
					final Text outkey1 = new Text();
					final Text outkey2 = new Text();
					outkey1.set(firstEdge);
					outkey2.set(lastEdge);
					context.write(outkey2, outkey1);
				}
			}
        }
	}

	public static class CountTriangleMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey1 = new Text();
		private Text outvalue1 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			outkey1.set(value);
			outvalue1.set(value + "," + "path2");
			context.write(outkey1, outvalue1);
		}
	}

	public static class CountTokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey1 = new Text();
		private Text outvalue1 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			boolean isFiltered = MAXFilter(10000, value);
			if(isFiltered) {
				final String[] followerString = value.toString().split(",");
				outkey1.set(value);
				outvalue1.set(value + "," + "edge");
				context.write(outkey1, outvalue1);
			}
		}

		public boolean MAXFilter(final int maxValue, final Text value) {
			final String[] followerString = value.toString().split(",");
			if (Integer.parseInt(followerString[0]) > maxValue) {
				return false;
			}
			if (Integer.parseInt(followerString[1]) > maxValue) {
				return false;
			}
			return true;
		}
	}

	public static class CountTriangleReducer extends Reducer<Text, Text, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			List<String> toMap = new ArrayList<>();
			List<String> fromMap = new ArrayList<>();
			for (final Text val : values) {
				final String[] followerString = val.toString().split(",");
				if (followerString[2].equals("path2")) {
					toMap.add(followerString[0] + "," + followerString[1]);
				}
				if (followerString[2].equals("edge")) {
					fromMap.add(followerString[0] + "," + followerString[1]);
				}
			}
			for (String firstEdge: toMap) {
				final String[] firstEdgeSplit = firstEdge.toString().split(",");
				for (String lastEdge: fromMap) {
					final String[] lastEdgeSplit = lastEdge.toString().split(",");
					if (firstEdge.equals(lastEdge)) {
//						final String path2 = entry1.getKey();
//						final Text outkey = new Text();
//						outkey.set(path2);
//						result.set(sum);
//						context.write(outkey, result);
						triangleCount = triangleCount + 1;
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Follower Count");
		job.setJarByClass(Follower.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		// Second Job
		final Job job2 = Job.getInstance(conf, "Count number of triangles");
		job2.setJarByClass(Follower.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
//		 ================
		job2.setReducerClass(CountTriangleReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, CountTriangleMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, CountTokenizerMapper.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.waitForCompletion(true);
		logger.info("TRIANGLE COUNT" + triangleCount/3);
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Follower(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
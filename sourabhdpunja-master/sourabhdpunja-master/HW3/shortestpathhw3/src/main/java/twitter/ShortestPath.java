package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

public class ShortestPath extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ShortestPath.class);

	public static class SRSMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		private Random rands = new Random();
		private Double percentage;
		private Long kValue;
		private final Text emitKey = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			String strPercentage = context.getConfiguration().get("filter_percentage");
			percentage = Double.parseDouble(strPercentage) / 100.0;
			kValue = Long.parseLong(context.getConfiguration().get("kvalue"));
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			long counterValue = context.getCounter(MySouCounter.RECORD_NUM).getValue();
			final String userString = value.toString().split(",")[0];
			if (counterValue < kValue && rands.nextDouble() < percentage) {
				context.getCounter(MySouCounter.RECORD_NUM).increment(1);
				emitKey.set(userString);
				context.write(NullWritable.get(), emitKey);
			}
		}
	}


// MAP REDUCE JOB TO PREPROCESS THE INPUT
    public static class AdjacencyListMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
			Text word = new Text();
			Text wordone = new Text();
			final String[] followerString = value.toString().split(",");
			word.set(followerString[0]);
			wordone.set(followerString[1]);
			context.write(word, wordone);
			word.clear();
			word.set("0");
			context.write(wordone, word);
        }
    }

    // OUTPUT POINTS IN THE FORM {TOVERTEX, STATUS, DISTANCEFROMSOURCE, ADJACENCYLIST}
    public static class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Text word = new Text();
        	int source = 2;
			String active;
			if (source == Integer.parseInt(key.toString())) {
				active = "A,0,";
			} else {
				active = "U,10000,";
			}
			for (final Text val : values) {
				if (val.toString().equals("0"))
					continue;
				active = active + val.toString()+",";
			}
			active = active.substring(0, active.length()-1);
			word.set(active);
			context.write(key, word);
        }
    }

    // SHORTEST PATH ALGORITHM FOR MAPPER
	public static class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text emitKey = new Text();
			Text emitValue = new Text();

			//Pass along the graph structure
			final String[] vertexSplit = value.toString().split(",");
			StringBuffer statusDistAdjacencyBuffer = new StringBuffer("");
			for (int i = 1; i < vertexSplit.length; i++) {
				if (i == 1 && vertexSplit[i].equals("A")) {
					statusDistAdjacencyBuffer.append("V,");
				}
				else {
					statusDistAdjacencyBuffer.append(vertexSplit[i] + ",");
				}
			}
			String statusDistAdjacency = statusDistAdjacencyBuffer.toString().substring(0, statusDistAdjacencyBuffer.length()-1);
			emitValue.set(statusDistAdjacency);
			String fromVertex = vertexSplit[0];
			String status = vertexSplit[1];
			emitKey.set(fromVertex);
			context.write(emitKey, emitValue);
			emitValue.clear();
			emitKey.clear();
			StringBuffer statusDistance;

			// Pass visited adjancency update
			if(status.equals("A") && vertexSplit.length > 3){
				for (int i = 3; i < vertexSplit.length; i++) {
					emitValue.clear();
					emitKey.clear();
					statusDistance = new StringBuffer("");
					emitKey.set(vertexSplit[i]);
					statusDistance.append(status);
					statusDistance.append(","+(Integer.parseInt(vertexSplit[2])+1));
					emitValue.set(statusDistance.toString());
					context.write(emitKey, emitValue);
				}
			}
		}
	}

	// REDUCER SHORTEST PATH ALGORITHM
	public static class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Text value = new Text();
			StringBuffer distanceStatusAdj = new StringBuffer("");

			for (Text val : values) {

				distanceStatusAdj = distanceStatusAdj.toString().equals("") ? new StringBuffer(val.toString()) : distanceStatusAdj;
				value.set(val);
				String[] vertexSplit = val.toString().split(",");
				String vertexStatus = vertexSplit[0];
				StringBuffer cloneDistStatusAdjList = new StringBuffer("");
				int dist;

				// Checking if status A which is active
				if (vertexStatus.equals("A")) {
					String[] valBufferArray = distanceStatusAdj.toString().split(",");
					dist = Integer.parseInt(vertexSplit[1]);
					for (int j = 0; j < valBufferArray.length; j++) {
						if (j == 0) {
							cloneDistStatusAdjList.append("A,");
						} else if (j == 1) {
							cloneDistStatusAdjList.append(dist+",");
							//Change global boolean
							int dist1 = Integer.parseInt(valBufferArray[j]);
							if (dist != dist1) { // IF distance changed then the counter is incremented
                                context.getCounter(MySouCounter.DISTANCE_CHANGED).increment(1);
							}
						}
						else {
							cloneDistStatusAdjList.append(valBufferArray[j]+",");
						}
					}
					value.set(cloneDistStatusAdjList.toString().substring(0, cloneDistStatusAdjList.length()-1));

				} else if (vertexStatus.equals("U") && vertexSplit.length > 2) { // Checking if status U which is active
					String[] valBufferArray = distanceStatusAdj.toString().split(",");
					for (int j = 0; j < vertexSplit.length; j++) {
						if (j < 2) {
							//Change global boolean
							int dist1 = Integer.parseInt(vertexSplit[j]);
							int dist2 = Integer.parseInt(valBufferArray[j]);
							if (j == 1 &&  dist1 != dist2) { // IF distance changed then the counter is incremented
                                context.getCounter(MySouCounter.DISTANCE_CHANGED).increment(1);
							}
							cloneDistStatusAdjList.append(valBufferArray[j]+",");
						}
						else {
							cloneDistStatusAdjList.append(vertexSplit[j]+",");
						}
					}
					// Remove the last from the string
					String writtenValue = cloneDistStatusAdjList.toString().substring(0, cloneDistStatusAdjList.length()-1);
					value.set(writtenValue);
				}

				else if (vertexStatus.equals("V")) { // Checking if status V which is active
					break;
				}

			}
			context.write(key, value);
		}
	}


	public int run(final String[] args) throws Exception {


		// Sample K Values Job
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "ShortestPath Count");
		job.setJarByClass(ShortestPath.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		// ================
		jobConf.set("filter_percentage", "30");
		jobConf.set("kvalue", "1");
		job.setMapperClass(SRSMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);


		// Second Job
		final Configuration conf2 = getConf();
		final Job job2 = Job.getInstance(conf2, "Get the input");
		job2.setJarByClass(ShortestPath.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem2 = FileSystem.get(conf);
		if (fileSystem2.exists(new Path(args[1]))) {
			fileSystem2.delete(new Path(args[1]), true);
		}
//		 ================
		job2.setMapperClass(AdjacencyListMapper.class);
		job2.setReducerClass(AdjacencyListReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path("output1"));

		job2.waitForCompletion(true);

		// Third Job Shortest Path
		int i = 0;
		//for (int i = 1; i < 3; i++) {
		boolean isDistanceChanged = true;
		long isDistanceChangedCounter;
		while (isDistanceChanged) {
			i++;

			final Job job3 = Job.getInstance(conf, "Get the shortestPath");
			job3.setJarByClass(ShortestPath.class);
			final Configuration jobConf3 = job3.getConfiguration();
			jobConf3.set("mapreduce.output.textoutputformat.separator", ",");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========
			/*final FileSystem fileSystem2 = FileSystem.get(conf);
			if (fileSystem2.exists(new Path(args[2]))) {
				fileSystem2.delete(new Path(args[2]), true);
			}*/

			final FileSystem fileSystem3 = FileSystem.get(conf);
			if (fileSystem3.exists(new Path("output"+(i-1)))) {
				fileSystem3.delete(new Path("output"+(i-1)), true);
			}

//		 ================
			job3.setMapperClass(ShortestPathMapper.class);
			job3.setReducerClass(ShortestPathReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job3, new Path("output"+i));
			FileOutputFormat.setOutputPath(job3, new Path("output"+(i+1)));

			job3.waitForCompletion(true);
			Counters cn = job3.getCounters();
			Counter c1 = cn.findCounter(MySouCounter.DISTANCE_CHANGED);
			isDistanceChangedCounter = c1.getValue();
			if (isDistanceChangedCounter == 0){
				isDistanceChanged = false;
			} else {
				isDistanceChanged = true;
			}
		}

		return 1;

	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}
		try {
			ToolRunner.run(new ShortestPath(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
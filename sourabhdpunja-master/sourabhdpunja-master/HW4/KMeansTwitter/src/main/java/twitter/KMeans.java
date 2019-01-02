package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KMeans extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(KMeans.class);

	//Create List of Centroids

    // KMEANS ALGORITHM FOR MAPPER
	public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
		private List<Double> listOfCentroids = new ArrayList<>();

		@Override
		public void setup(Context context) throws IOException {
			URI[] fileCache = context.getCacheFiles();
			if (fileCache != null && fileCache.length > 0) {
				for (int i = 0; i < fileCache.length; i++) {
					final Configuration jobConf = context.getConfiguration();
					final FileSystem fileSystem = FileSystem.get(fileCache[i], jobConf);
					String URI = fileCache[i].toString();
					Path centroidPath = new Path(URI);
					try{
						BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(centroidPath)));

						for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
							String centroid = line.split(",")[0];
							Double centroidDouble = Double.parseDouble(centroid);
							listOfCentroids.add(centroidDouble);
						}

						bufferedReader.close();
					}
					catch(Exception e){
						System.err.println("Error: Target File Cannot Be Read");
					}


				}
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text emitKey = new Text();
			Text emitValue = new Text();

			//Pass along the graph structure
			final String[] followerCountSplit = value.toString().split(",");
			Double followerCount = Double.parseDouble(followerCountSplit[1]);

			Double minCentroid = listOfCentroids.get(0);
			Double errorDist = Math.abs(followerCount - listOfCentroids.get(0));
			for (Double centroid: listOfCentroids) {
				Double currentCentroidDist = Math.abs(followerCount - centroid);
				if (currentCentroidDist < errorDist) {
					minCentroid = centroid;
					errorDist = currentCentroidDist;
				}
			}
			emitKey.set(minCentroid.toString());
			emitValue.set(followerCount.toString());
			context.write(emitKey, emitValue);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Double centroid: listOfCentroids) {
				context.write(new Text(centroid.toString()), new Text("0"));
			}
		}
	}

	// KMEANS ALGORITHM FOR REDUCER
	public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Double centroid = Double.parseDouble(key.toString());
			Double newCentroid;
			long newsse;
			List<Double> listOfFollowerCounts = new ArrayList<>();
			double sum = 0;

			for (Text val : values) {
				Double followerCount = Double.parseDouble(val.toString());
				if (followerCount == 0d) {
					continue;
				}
				Double diff = Math.abs(followerCount);
				sum += diff;
				listOfFollowerCounts.add(followerCount);
			}
			if (listOfFollowerCounts.size() == 0) {
				newCentroid = centroid;
			} else {
				double totalPoints = listOfFollowerCounts.size();
				newCentroid = sum/totalPoints;
			}

			sum = 0;
			for (Double followerCount: listOfFollowerCounts) {
				Double error = Math.abs(followerCount - newCentroid);
				sum += Math.pow(error, 2);
			}
			newsse = (long) (sum * 1000);
			context.getCounter(MySouCounter.SSE).increment(newsse);
			Text emitKey = new Text();
			emitKey.set(newCentroid.toString());
			context.write(emitKey, key);
		}
	}


	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		// Third Job Shortest Path
		int i = 0;
		//for (int i = 1; i < 3; i++) {
		boolean isConverged = false;
		double oldSSE = 0;
		double newSSE;
		while (!isConverged && i < 10) {
			i++;
			final Job job = Job.getInstance(conf, "Get the Clusters");
			job.setJarByClass(KMeans.class);
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========

			final FileSystem fileSystem3 = FileSystem.get(conf);
			int j = i-2;
//			if (j != 0) {
//				if (fileSystem3.exists(new Path(args[1]+j))) {
//					fileSystem3.delete(new Path(args[1]+j), true);
//				}
//			}
			j = i - 1;
			Path centroidsFile = new Path(args[1] + j);
			final FileSystem fileSystem = FileSystem.get(centroidsFile.toUri(),conf);
			RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(centroidsFile, false);

			while (remoteIterator.hasNext()) {
				URI uri = remoteIterator.next().getPath().toUri();
				job.addCacheFile(uri);
			}

//		 ================
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+i));

			job.waitForCompletion(true);
			Counters cn = job.getCounters();
			Counter c1 = cn.findCounter(MySouCounter.SSE);
			newSSE = c1.getValue()/1000d;
			logger.info("OLD COUNTER: " + oldSSE);
			if (newSSE == oldSSE){
				isConverged = true;
			} else {
				isConverged = false;
				oldSSE = newSSE;
			}
			logger.info("ITERATION #: " + i);
			logger.info("NEW COUNTER: " + newSSE);
		}
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}
		try {
			ToolRunner.run(new KMeans(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
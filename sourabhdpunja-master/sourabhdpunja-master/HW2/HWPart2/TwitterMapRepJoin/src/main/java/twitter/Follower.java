package twitter;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Follower extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(Follower.class);
	private static int triangleCount = 0;


    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private HashMap<String, List<String>> broadcastEdgesInfo = new HashMap<String, List<String>>();
        private int triangleLocalCount = 0;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			try {
				URI[] cacheContext = context.getCacheFiles();
				if (cacheContext == null || cacheContext.length == 0) {
					throw new RuntimeException(
							"Context is not set in DistributedCache");
				}
				URI fileEdgeURI = cacheContext[0];
				if (fileEdgeURI != null) {
                    BufferedReader reader = new BufferedReader(new FileReader("./edges.csv"));
					String edgeLine;
					// For each record in the user file
					while ((edgeLine = reader.readLine()) != null) {
						final Text edge = new Text();
						edge.set(edgeLine);
						boolean isFiltered = MAXFilter(1000, edge);
						if (isFiltered) {
							String fromEdge = edgeLine.split(",")[0];
							String toEdge = edgeLine.split(",")[1];
							if (broadcastEdgesInfo.containsKey(fromEdge)) {
								broadcastEdgesInfo.get(fromEdge).add(toEdge);
							} else {
								broadcastEdgesInfo.put(fromEdge, new ArrayList<String>() {{
									add(toEdge);
								}});
                            }
                        }
                    }
                }
            } catch(IOException e){
                throw new RuntimeException(e);
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

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            boolean isFiltered = MAXFilter(1000, value);
            String fromEdge = value.toString().split(",")[0];
            String toEdge = value.toString().split(",")[1];
            if(isFiltered) {
                if(broadcastEdgesInfo.containsKey(toEdge)){
                    List<String> matches = broadcastEdgesInfo.get(toEdge);
                    for(String edge : matches){
                        if(broadcastEdgesInfo.containsKey(edge) && broadcastEdgesInfo.get(edge).contains(fromEdge)){
                            triangleLocalCount++;
                        }
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            triangleCount = triangleCount + triangleLocalCount;
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
        job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String cacheFilePath = args[0] + "/edges.csv";
        job.addCacheFile(new URI(cacheFilePath));
		job.waitForCompletion(true);
		logger.info("TRIANGLE COUNT" + triangleCount/3);
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new Follower(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
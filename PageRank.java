package PageRank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRank {
	
	public static final String SPECIAL_CHAR = "#";
	
	public static void main(String[] args) throws Exception {
		
		if(args.length < 2) {
			System.err.println("Missing input parameters !!");
			System.exit(1);
		}
		
		String input = args[0]; // input file location
		String output = args[1]; // output location

		String resultDir = output + "/results/"; // location to store finalresults
		String tmpDir = output + "/tmp/"; // location to store temporary results

		// Filenames to output from various map-reduce jobs
		String adjacencyGraphOutFile = "PageRank.outlink.out";
		String parsedOutFile = "PageRank.parsed.out";
		String numOfPagesOutFile = "PageRank.n.out";
		String iter1SortedFile = "PageRank.iter1.sorted.out";
		String iter8SortedFile = "PageRank.iter8.sorted.out";

		int noOfIterations = 8; // number of page rank iterations to run
		String[] pageRankIterOutFile = new String[noOfIterations + 1];
		for (int i = 0; i <= noOfIterations; i++) {
			pageRankIterOutFile[i] = "PageRank.iter" + Integer.toString(i)
					+ ".out";
		}

		PageRank pageRank = new PageRank();
		
		// Map-reduce job to parse input xml and remove red links (by creating in-graph)
		pageRank.parseXml(input, tmpDir + parsedOutFile);

		// Map-reduce job to create adjacency graph (out-graph)
		pageRank.generateAdjacencyGraph(tmpDir + parsedOutFile, tmpDir
				+ adjacencyGraphOutFile);

		// Map-reduce job to count total number of pages in input file
		int count = pageRank.calculateTotalPages(tmpDir + adjacencyGraphOutFile, tmpDir
				+ numOfPagesOutFile);

		// Map-reduce job to add initial rank to the output of adjacency graph map-reduce job
		pageRank.pageRankIter0(tmpDir + adjacencyGraphOutFile, tmpDir
				+ pageRankIterOutFile[0], count);

		for (int i = 1; i <= noOfIterations; i++) {
			// Map-reduce job to calculate page rank
			pageRank.calculatePageRank(tmpDir + pageRankIterOutFile[i - 1],
					tmpDir + pageRankIterOutFile[i], count);
		}

		// Map-reduce job to sort results returned by page rank job
		pageRank.orderRank(tmpDir + pageRankIterOutFile[1], tmpDir
				+ iter1SortedFile, count);
		pageRank.orderRank(tmpDir + pageRankIterOutFile[8], tmpDir
				+ iter8SortedFile, count);

		// Merge various part files in output into one output file for each map-reduce job
		PageRankUtilities.mergeOutputFiles(tmpDir + adjacencyGraphOutFile, resultDir, adjacencyGraphOutFile);
		PageRankUtilities.mergeOutputFiles(tmpDir + numOfPagesOutFile, resultDir, numOfPagesOutFile);
		PageRankUtilities.mergeOutputFiles(tmpDir + iter1SortedFile, resultDir, pageRankIterOutFile[1]);
		PageRankUtilities.mergeOutputFiles(tmpDir + iter8SortedFile, resultDir, pageRankIterOutFile[8]);
	}

	// mapper output: <k, v> = <outlink, title>
	// reducer output: <k, v> = <title, outlink> or <title, special_char>
	public void parseXml(String input, String output) throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		conf.set("special_char", SPECIAL_CHAR);

		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setInputFormat(XmlInputFormat.class);
		conf.setMapperClass(XMLParserRedLinkRemoverMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(XMLParserRedLinkRemoverReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
	}

	// mapper output: <k, v> = <title, outlink>
	// reducer output: <k, v> = <title, outlinks>
	public void generateAdjacencyGraph(String input, String output)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		conf.set("special_char", SPECIAL_CHAR);

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setMapperClass(AdjacencyOutlinkGraphCreatorMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(AdjacencyOutlinkGraphCreatorReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
	}

	// mapper output: <k, v> = <1, null>
	// reducer output: <k, v> = <N=count, null>
	public int calculateTotalPages(String input, String output)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setMapperClass(PageCounterMapper.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(PageCounterReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		
		conf.setNumReduceTasks(1);

		JobClient.runJob(conf);
		
		return PageRankUtilities.readCountFromFile(output);
	}

	// mapper output: <k, v> = <title, outlinks>
	// reducer output: <k, v> = <title, 1/count \t outlinks>
	public void pageRankIter0(String input, String output, int count) throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		conf.set("count", Integer.toString(count));

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setMapperClass(PageRankIter0Mapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(PageRankIter0Reducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
	}

	// mapper output:
	// if page has outlinks then <k,v> = <page tile, special_char \t outlinks>
    // else <k,v> = <page tile, special_char>
	// Also, <k,v> = <outlink, pageVote>
	// reducer output:
	// if sink page then <k,v> = <title, pageRank>
    // else <k,v> = <title, pageRank \t outlinks>
	public void calculatePageRank(String input, String output, int count) throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		conf.set("count", Integer.toString(count));
		conf.set("special_char", SPECIAL_CHAR);

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setMapperClass(PageRankCalculatorMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(PageRankCalculatorReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		//conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
	}

	// mapper output: <k, v> = <pageRank, title>
	// reducer output: <k, v> = <title, pageRank>
	public void orderRank(String input, String output, int count)
			throws IOException {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJarByClass(PageRank.class);
		conf.set("count", Integer.toString(count));	

		FileInputFormat.setInputPaths(conf, new Path(input));
		conf.setMapperClass(PageRankResultSorterMapper.class);

		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setReducerClass(PageRankResultSorterReducer.class);

		conf.setMapOutputKeyClass(DoubleWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		// sort the keys of reducer
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);

		conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(DoubleWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable ip1 = (DoubleWritable) w1;
			DoubleWritable ip2 = (DoubleWritable) w2;
			return ip1.compareTo(ip2);
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(DoubleWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable d1 = (DoubleWritable) w1;
			DoubleWritable d2 = (DoubleWritable) w2;
			int cmp = d1.compareTo(d2);
			return cmp * -1; // descending order
		}
	}
}
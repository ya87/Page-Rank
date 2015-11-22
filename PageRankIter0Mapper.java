package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class PageRankIter0Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
    	String line = value.toString();
        int titleEndIndex = line.indexOf("\t");
        String title = line.substring(0, titleEndIndex);

        String outLinks = line.substring(titleEndIndex + 1);

        //<k,v> = <title, outlinks>
        output.collect(new Text(title), new Text(outLinks));
    }
}

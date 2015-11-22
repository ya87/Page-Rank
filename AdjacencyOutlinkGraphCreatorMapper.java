package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class AdjacencyOutlinkGraphCreatorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] strArr = value.toString().split("\t");

        //<k,v> = <title, outlink>
        output.collect(new Text(strArr[0]), new Text(strArr[1]));
    }
}
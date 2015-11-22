package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class PageRankResultSorterMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {

    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {

        String line = value.toString();

        int titleEndIndex = line.indexOf("\t");
        int rankEndIndex = line.indexOf( "\t", titleEndIndex + 1);

        if (rankEndIndex == -1) {
            rankEndIndex = line.length();
        }

        String title = line.substring(0, titleEndIndex);
        double pageRank = Double.parseDouble(line.substring(titleEndIndex + 1,rankEndIndex));

        // <k,v> = <pageRank, title>
        output.collect(new DoubleWritable(pageRank), new Text(title));
    }
}
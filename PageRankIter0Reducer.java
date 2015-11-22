package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankIter0Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String count;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double intialPageRank = 1.0 / Integer.parseInt(count);
        String outLinks = "";

        if(values.hasNext()) {
           outLinks = values.next().toString();
        }

        //<k,v> = <title, 1/count \t outlinks>
        output.collect(key, new Text(Double.toString(intialPageRank) + "\t" + outLinks) );
    }
}

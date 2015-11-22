package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankResultSorterReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public static String count = "1.0";

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
    }

    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        int n = Integer.parseInt(count);
        double threshold = 5.0 / n;

        //if pageRank > threshold then <k,v> = <title, pageRank>
        while(values.hasNext()) {
            double pageRank = Double.parseDouble(key.toString());
            if (pageRank >  threshold)
                output.collect(values.next(), key);
            else
               return;
        }
    }
}

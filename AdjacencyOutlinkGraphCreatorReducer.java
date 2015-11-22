package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class AdjacencyOutlinkGraphCreatorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String special_char;

    public void configure(JobConf jobConf){
        special_char = jobConf.get("special_char");
    }
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder outLinks = new StringBuilder();

        //appending all outlinks in one string separated by tab character 
        while(values.hasNext()) {
            String outLink = values.next().toString();

            if (!outLink.equals(special_char)) {
                outLinks.append(outLink + "\t");
            }
        }

        //removing last tab character
        if (outLinks.length() > 0) {
            outLinks.deleteCharAt(outLinks.length() - 1);
        }

        //<k,v> = <title, outlinks>
        output.collect(key, new Text(outLinks.toString()));
    }
}
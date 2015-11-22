package PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class XMLParserRedLinkRemoverReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public static String special_char;

    public void configure(JobConf jobConf){
        special_char = jobConf.get("special_char");
    }
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<String> set = new HashSet<String>();

        //remove duplicate out links
        while (values.hasNext()) {
            set.add(values.next().toString());
        }

        //remove red links
        //<k,v> = <title, outlink> or <title, special_char>
        if (set.contains(special_char)){
        	Iterator<String> it = set.iterator();
        	while(it.hasNext()) {
        		String inLink = it.next();
        		if(!inLink.equals(special_char)) {
        			output.collect(new Text(inLink), key);
        		} else {
        			output.collect( key, new Text(special_char));
        		}
        	}
        }
    }
}
package PageRank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.dom4j.Document;
import org.dom4j.DocumentException;


public class XMLParserRedLinkRemoverMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private static final Pattern WIKI_LINK_PATTERN1 = Pattern.compile("\\[\\[(.*?)\\]\\]"); 
	private static final Pattern WIKI_LINK_PATTERN2 = Pattern.compile("\\[\\[([^\\[]*?)\\]\\]");

    public static String special_char;

    public void configure(JobConf jobConf){
        special_char = jobConf.get("special_char");
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	try {
	        String page = value.toString();
	
	        Document dom = org.dom4j.DocumentHelper.parseText(page);
	        String title = (dom.selectSingleNode("/page/title").getText()).replace(" ", "_"); 
	        String text  = dom.selectSingleNode("/page//text").getText();
	        
	        Matcher m1 = WIKI_LINK_PATTERN1.matcher(text);
	        Matcher m2 = WIKI_LINK_PATTERN2.matcher(text);
	        
	        Set<String> outlinks = new HashSet<String>();
	
	        while(m1.find()) {
	            String temp = m1.group(1);
	            
	            if(temp != null && !temp.isEmpty()) {
	            	String outLink = null;
	                if(temp.contains("|")) {
	                    outLink = temp.substring(0, temp.indexOf("|")).replace(" ", "_");
	                } else {
	                    outLink = temp.replace(" ", "_");   
	                }
	                
	                if (outLink != null && !outLink.equals(title)) {
                        outlinks.add(outLink);
                    }
	            }
	        }
	        
	        while(m2.find()) {
	            String temp = m2.group(1);
	            
	            if(temp != null && !temp.isEmpty()) {
	            	String outLink = null;
	                if(temp.contains("|")) {
	                    outLink = temp.substring(0, temp.indexOf("|")).replace(" ", "_");
	                } else {
	                    outLink = temp.replace(" ", "_");   
	                }
	                
	                if (outLink != null && !outLink.equals(title)) {
                        outlinks.add(outLink);
                    }
	            }
	        }
	        
	        //adding record <title, special_char> to identify red links
	        output.collect(new Text(title), new Text(special_char));
	        
	        //this loop returns <k, v> as <outlink, title>
	        for(String outLink: outlinks) {
	        	output.collect(new Text(outLink), new Text(title));
	        }
	        
		} catch(DocumentException e) {
			e.printStackTrace();
		}
    }
}
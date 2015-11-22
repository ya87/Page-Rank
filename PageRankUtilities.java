package PageRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class PageRankUtilities {
	public static Integer readCountFromFile(String filePath) {
		int count = 0;
		FileSystem fs = null;
		Path path = null; 
		BufferedReader br = null;
				
		try {
			//fs = FileSystem.get(new Configuration());
			path = new Path(filePath + "/part-00000");
			fs = FileSystem.get(path.toUri(), new Configuration());

			if(fs.exists(path)) {
			    br = new BufferedReader(new InputStreamReader(fs.open(path)));
			    String line = br.readLine();
			    if(line != null && !line.isEmpty()) {
			    	count = Integer.parseInt(line.split("=")[1]); //"N=123" extracting 123
			    }
			} else {
				System.err.println("File containing count does not exist !!");
				System.exit(1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null) {
					br.close();
				}
				if(fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return count;
	}
	
	public static void mergeOutputFiles(String tmpOutFileLoc, String outFileLoc, String outFileName) {
		FileSystem fs = null;
		
		try {
			Configuration config = new Configuration();
			Path srcPath = new Path(tmpOutFileLoc);
		    Path dstPath = new Path(outFileLoc + outFileName);
			fs = FileSystem.get(srcPath.toUri(), config);
		    
		    if (!(fs.exists(srcPath))) {
		        System.err.println("Source Path " + tmpOutFileLoc + " does not exists!");
		        System.exit(1);
		    }
	
		    FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, config, null);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void writeToFile(String text, String filename) {
		PrintWriter pw = null; 
		
		try {
			pw = new PrintWriter(filename);
			pw.println(text);
			pw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}

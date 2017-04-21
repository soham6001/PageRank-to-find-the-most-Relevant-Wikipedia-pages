package wikilinksandred;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class  WikiLinks {

    public static long count = 0;

    public static void main(String[] args) throws Exception {
        WikiLinks mainObject = new WikiLinks();

        String outputGraph = args[1] + "/graph/";
		String tempFiles = args[1] + "/temp/";
		String OutputStage1 = "WikiLinks.stage1.out";
		mainObject.OutlinkGenrationJob1(args[0], tempFiles + OutputStage1);
		mainObject.OutlinkGenrationJob2(tempFiles + OutputStage1, tempFiles + "/temp1");
        mainObject.MergeFiles(tempFiles + "/temp1", outputGraph + "part-r-00000");


    }

    public void OutlinkGenrationJob1(String input, String output) throws IOException {

    	JobConf conf = new JobConf(WikiLinks.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        conf.setJarByClass(WikiLinks.class);


        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(XmlInputFormat.class);
        conf.setMapperClass(OutlinkMapperStage1.class);



        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(OutlinkReducerStage1.class);



        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);


        JobClient.runJob(conf);
    }

    public void OutlinkGenrationJob2(String input, String output) throws IOException {

    	JobConf conf = new JobConf(WikiLinks.class);
        conf.setJarByClass(WikiLinks.class);


        FileInputFormat.setInputPaths(conf, new Path(input));
        conf.setMapperClass(OutlinkMapperStage2.class);


        FileOutputFormat.setOutputPath(conf, new Path(output));
        conf.setReducerClass(OutlinkReducerStage2.class);


        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);


        JobClient.runJob(conf);
    }


  private void MergeFiles(String input, String output) throws IOException {
		String fileName = input + "/part-";
		NumberFormat nf = new DecimalFormat("00000");
		FileSystem outFS = null;

		try {

			Path outFile = new Path(output);
			outFS = outFile.getFileSystem(new Configuration());
			if (outFS.exists(outFile)) {
				// System.out.println(outFile + " already exists");
				System.exit(1);
			}

			FSDataOutputStream out = outFS.create(outFile);

			Path inFile = new Path(fileName + nf.format(0));
			FileSystem inFS = inFile.getFileSystem(new Configuration());

			if (!inFS.exists(inFile)) {
				fileName = input + "/part-";
			}

			// This will generate file names -00001 to -00999, I hope this is
			// sufficient
			for (int i = 0;; i++) {

				inFile = new Path(fileName + nf.format(i));
				inFS = inFile.getFileSystem(new Configuration());

				if (inFS.isFile(inFile)) {

					int bytesRead = 0;
					byte[] buffer = new byte[4096];

					FSDataInputStream in = inFS.open(inFile);

					while ((bytesRead = in.read(buffer)) > 0) {
						out.write(buffer, 0, bytesRead);
					}

					in.close();
				} else {
					break;
				}

				inFS.close();
			}

			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				outFS.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}

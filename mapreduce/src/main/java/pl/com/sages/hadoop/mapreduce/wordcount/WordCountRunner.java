package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Path outputPath = new Path(args[1]);
        System.out.println(outputPath);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = createJob(inputPath, outputPath, conf);   //conf - plik komnifuracyjny - mozna ja ustalic conf.set - context.getcondiguration...

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(final Path inputPath, final Path outputPath, final Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "word count");  //word count - nazwa ktora sie nam bedzie wyswietlac
        job.setJarByClass(WordCountRunner.class);      // chcemy czytac klase z konkretnego jara
        job.setMapperClass(WordCountMapper.class);  //map reduce combiner
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);          //klasy wyjsciowe
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(5);       //tu mozna zdefiniowac liczbe partow rozwiazania
        FileInputFormat.addInputPath(job, inputPath);       //sciezka wyjscia / wejscia
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}

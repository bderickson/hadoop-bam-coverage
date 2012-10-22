package edu.wustl.genome.hadoop;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.AlignmentBlock;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Iterator;
import java.lang.String;

import edu.wustl.genome.hadoop.GenomicPositionWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BAMCoverage {

    public static class BAMCoverageMapper
            extends Mapper<LongWritable, SAMRecordWritable, GenomicPositionWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static GenomicPositionWritable gpw = new GenomicPositionWritable();

        public void map(LongWritable key, SAMRecordWritable value, Context context) 
                throws IOException, InterruptedException {
            SAMRecord record = value.get();
            String referenceName = record.getReferenceName();
            List<AlignmentBlock> alignmentBlocks = record.getAlignmentBlocks();
            Iterator<AlignmentBlock> itr = alignmentBlocks.iterator();

            AlignmentBlock alignment_block;
            int reference_start;
            int block_length;
            int position;

            while ( itr.hasNext() ) {
                alignment_block = itr.next();
                reference_start = alignment_block.getReferenceStart();
                block_length = alignment_block.getLength();
                for (position = reference_start; position < reference_start + block_length; position++) {
                    gpw.chromosome = referenceName;
                    gpw.position = position;
                    context.write(gpw, one);
                }
            }
        }
    }

    public static class IntSumReducer 
            extends Reducer<GenomicPositionWritable, IntWritable, GenomicPositionWritable, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(GenomicPositionWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BAMCoverage <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "BAM Coverage");
        job.setJarByClass(BAMCoverage.class);
        job.setJobName("Per position genomic coverage");

        BAMInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputKeyClass(GenomicPositionWritable.class);
        job.setInputFormatClass(AnySAMInputFormat.class);

        job.setMapperClass(BAMCoverageMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(GenomicPositionWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

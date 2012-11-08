package edu.wustl.genome.hadoop.coverage;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.AnySAMInputFormat;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.AlignmentBlock;

import java.io.IOException;
import java.util.Iterator;
import java.lang.String;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configured;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;

public class BAMCoverage {

    private static final Schema coverageSchema = Schema.parse(
        "{ \"namespace\": \"edu.wustl.genome.hadoop.coverage\", " +
        "\"type\" : \"record\", \"name\" : \"PerPositionCoverage\", \"fields\" : [ " +
        "{\"name\" : \"chromosome\", \"type\" : \"string\" }, {\"name\" : \"position\", " +
        "\"type\" : \"int\" }, { \"name\" : \"depth\", \"type\" : \"int\" } ] }"
    );

    public static class BamCoverageMapper extends Mapper<LongWritable, SAMRecordWritable, AvroKey<String>, AvroValue<Integer>> {

        private HashMap<String, LinkedHashMap<Integer, Integer>> outputs = new HashMap<String, LinkedHashMap<Integer, Integer>>();
        private static final Integer one = new Integer(1);
        private static final AvroValue<Integer> avroValue = new AvroValue<Integer>();
        private AvroKey<String> avroKey = new AvroKey<String>();

        @Override
        public void map(LongWritable key, SAMRecordWritable value, Context context)
                throws IOException, InterruptedException {
            SAMRecord record = value.get();
            String referenceName = record.getReferenceName();
            List<AlignmentBlock> alignmentBlocks = record.getAlignmentBlocks();
            AlignmentBlock alignmentBlock;
            Iterator<AlignmentBlock> itr = alignmentBlocks.iterator();

            LinkedHashMap<Integer, Integer> chrCoverage = outputs.get(referenceName);
            if (chrCoverage == null) {
                chrCoverage = new LinkedHashMap<Integer, Integer>();
                outputs.put(referenceName, chrCoverage);
            }

            Integer readStart = new Integer(-1);
            Integer referenceStart;
            Integer blockLength;
            Integer position;

            while (itr.hasNext()) {
                alignmentBlock = itr.next();
                referenceStart = alignmentBlock.getReferenceStart();
                if (readStart == null || readStart <= 0) {
                    readStart = referenceStart;
                }
                blockLength = alignmentBlock.getLength();

                for (position = referenceStart; position < referenceStart + blockLength; position++) {
                    Integer coverage = chrCoverage.get(position);
                    if (coverage == null) {
                        chrCoverage.put(position, one);
                    }
                    else {
                        chrCoverage.put(position, (coverage + one));
                    }
                }
            }

            outputRecordsWithKeyLessThan(referenceName, readStart, context);
        }

        // Write out coverage for any genomic position prior to the provided position
        private void outputRecordsWithKeyLessThan(String chr, Integer maxPosition, Context context) 
                throws IOException, InterruptedException {
            LinkedHashMap<Integer, Integer> chrCoverage = outputs.get(chr);
            Set<Integer> keys = chrCoverage.keySet();
            Iterator<Integer> iterator = keys.iterator();
            while (iterator.hasNext()) {
                Integer position = iterator.next();
                if (maxPosition != null && position >= maxPosition) {
                    break;
                }
                Integer coverage = chrCoverage.get(position);
                String chrPosition = new String(chr + ":" + String.valueOf(position));
                avroKey.datum(chrPosition);
                avroValue.datum(coverage);
                context.write(avroKey, avroValue);
                iterator.remove();
            }
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            while(context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }

            // Write out any remaining output key/value pairs
            Set<String> chromosomes = outputs.keySet();
            Iterator<String> iterator = chromosomes.iterator();
            while (iterator.hasNext()) {
                String chr = iterator.next();
                outputRecordsWithKeyLessThan(chr, null, context);
            }

            cleanup(context);
        }
    }

    public static class AvroBamCoverageReducer
            extends AvroReducer<Utf8, Integer, GenericRecord> {

        @Override
        public void reduce(Utf8 key, Iterable<Integer> iterable,
                AvroCollector<GenericRecord> collector, Reporter reporter)
                throws IOException {
            int sum = 0;
            Iterator<Integer> iterator = iterable.iterator();
            while(iterator.hasNext()) {
                Integer value = iterator.next();
                sum += value.intValue();
            }

            GenericRecord record = new GenericData.Record(coverageSchema);
            String[] parts = key.toString().split(":");
            record.put("chromosome", parts[0]);
            record.put("position", new Integer(parts[1]));
            record.put("depth", sum);
            collector.collect(record);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BAMCoverage <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "BAM Coverage");
        job.setJarByClass(BAMCoverage.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outputPath = new Path(otherArgs[1]);
        outputPath.getFileSystem(conf).delete(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(BamCoverageMapper.class);
        job.setInputFormatClass(AnySAMInputFormat.class);
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema mapPairSchema = Pair.getPairSchema(stringSchema, intSchema);
        AvroJob.setMapOutputSchema(conf, mapPairSchema);

        AvroJob.setReducerClass(conf, AvroBamCoverageReducer.class);
        AvroJob.setOutputSchema(conf, coverageSchema);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

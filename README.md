hadoop-bam-coverage
===================

Simple Hadoop app using Maven and the HadoopBAM library.

Mapper spits out genomic position (chromosome, start, stop) and coverage pairs. Reducer aggregates the coverage for each genomic position. Result is one record per position and the total coverage at that position.

A few notes:
The mapper produces A LOT of records. Each position of each read is an output record. For a whole genome bam, we're talking 100 billion records. Spilling is a major concern.

The Java heap size needs to be increased using the HADOOP_HEAPSIZE environment variable, especially with whole genome bams. I found that 4GB was sufficient for a 80GB bam file.

Code is included for a GenomicPositionWritable. It's not currently used because it ends up using more memory than delimited text.

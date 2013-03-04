hadoop-bam-coverage
===================

Simple Hadoop app using Maven and the HadoopBAM library.

Mapper spits out genomic position (chromosome, start, stop) and coverage pairs. Reducer aggregates the coverage for each genomic position. Result is one record per position and the total coverage at that position.

A few notes:

The mapper produces A LOT of records. Each position of each read is an output record. For a whole genome bam, we're talking 100 billion records. Spilling is a major concern.

The Java heap size needs to be increased using the HADOOP_HEAPSIZE environment variable, especially with whole genome bams. I found that 4GB was sufficient for a 80GB bam file.

Code is included for a GenomicPositionWritable. It's not currently used because it ends up using more memory than delimited text, though I felt this shouldn't be the case.

The POM file is set up to create a jar with dependencies included in addition to a "regular" jar without those dependencies. I found that this made managing jars on Hadoop nodes easier. The flip side is that the jar is much larger, but I considered it a worthwhile trade. Just run "mvn clean package" to compile and package.

package edu.wustl.genome.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenomicPositionWritable implements WritableComparable<GenomicPositionWritable> {
    public String chromosome;
    public Integer position;

    public GenomicPositionWritable() {};

    public GenomicPositionWritable(String chromosome, Integer position) {
        this.chromosome = chromosome;
        this.position = position;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, chromosome);
        WritableUtils.writeVInt(out, position);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chromosome = WritableUtils.readString(in);
        position = WritableUtils.readVInt(in);
    }

    @Override
    // This has to be called on each key/value pair emmitted by map during the shuffle.
    // Should be optimized as much as possible.
    public int compareTo(GenomicPositionWritable o) {
        int chr_compare = this.chromosome.compareTo(o.chromosome);
        if (chr_compare == 0) {
            return this.position.compareTo(o.position);
        }
        else {
            return chr_compare;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GenomicPositionWritable)) {
            return false;
        }
        GenomicPositionWritable other = (GenomicPositionWritable)o;
        return (this.chromosome.equals(other.chromosome)) && (this.position.equals(other.position));
    }

    @Override
    public String toString() {
        return "Chromosome " + chromosome + ", position " + position;
    }

    /*
    @Override
    // This gets used during partitioning and it's recommended it be overridden.
    public int hashCode() {
        return;
    }
    */
}

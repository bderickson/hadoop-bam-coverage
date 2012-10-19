package edu.wustl.genome.hadoop;

import org.apache.hadoop.io.WritableComparable;

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
        out.writeUTF(chromosome);
        out.writeInt(position);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        chromosome = in.readUTF();
        position = in.readInt();
    }

    @Override
    // This has to be called on each key/value pair emmitted by map during the shuffle.
    // Should be optimized as much as possible.
    public int compareTo(GenomicPositionWritable o) {
        if (o.chromosome.equals(this.chromosome)) {
            return this.position.compareTo(o.position);
        }
        else {
            return this.chromosome.compareTo(o.chromosome);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GenomicPositionWritable)) {
            return false;
        }
        GenomicPositionWritable other = (GenomicPositionWritable)o;
        return (this.chromosome == other.chromosome) && (this.position == other.position);
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

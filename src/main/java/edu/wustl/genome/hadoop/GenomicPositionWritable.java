package edu.wustl.genome.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenomicPositionWritable implements WritableComparable<GenomicPositionWritable> {
    private String chromosome;
    private int position;

    private GenomicPositionWritable() {};

    public GenomicPositionWritable(String chromosome, int position) {
        this.chromosome = chromosome;
        this.position = position;
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(chromosome);
        out.writeInt(position);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readLine();
        position = in.readInt();
    }

    public String getChromosome() {
        return chromosome;
    }

    public int getPosition() {
        return position;
    }

    public int compareTo(GenomicPositionWritable o) {
        if (o.getChromosome().equals(this.getChromosome())) {
            return (o.getPosition() == this.getPosition() ? 0 : (o.getPosition() < this.getPosition() ? -1 : 1));
        }
        else {
            return o.getChromosome().compareTo(this.getChromosome());
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof GenomicPositionWritable)) {
            return false;
        }
        GenomicPositionWritable other = (GenomicPositionWritable)o;
        return (this.chromosome == other.getChromosome()) && (this.position == other.getPosition());
    }

    public String toString() {
        return "Chromosome " + chromosome + ", position " + position;
    }

    /*public static GenomicPositionWritable read(DataInput in) throws IOException {
        GenomicPositionWritable gpw = new GenomicPositionWritable();
        gpw.readFields(in);
        return gpw;
    }*/
}

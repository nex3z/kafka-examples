package com.nex3z.examples.kafka.data;

public class CounterRecord {

    private String name;

    private int count;

    public CounterRecord(String name, int count) {
        this.name = name;
        this.count = count;
    }

    public CounterRecord() { }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CounterRecord{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}

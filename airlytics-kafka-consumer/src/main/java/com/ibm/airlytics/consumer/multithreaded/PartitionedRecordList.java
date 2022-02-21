package com.ibm.airlytics.consumer.multithreaded;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class PartitionedRecordList<K, V> extends ArrayList<ArrayList<ConsumerRecord<K, V>>> {

    public interface ConsumerRecordPartitioner<K, V> {
        public int whichPartition(ConsumerRecord<K, V> record);
    }

    private ConsumerRecordPartitioner<K, V> partitioner;

    public PartitionedRecordList(int partitions, ConsumerRecordPartitioner<K, V> partitioner) {
        super(partitions);

        this.partitioner = partitioner;

        for (int i=0; i<partitions; ++i) {
            add(new ArrayList<>());
        }
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        // No need to run partitioner (potentially expensive) logic if there is only one partition
        if (size() == 1) {
            get(0).add(record);
        } else get(partitioner.whichPartition(record)).add(record);
    }

    public List<ConsumerRecord<K, V>> getRecords(int partition) {
        return get(partition);
    }

    public long totalSize() {
        long result = 0;
        for (List partition : this)
            result += partition.size();
        return result;
    }
}
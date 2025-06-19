package org.tpatel.kinesis.firehose.publisher;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordRequest;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;

public class KinesisRecordPublisher {

    FirehoseClient firehosePublisher;

    public KinesisRecordPublisher(FirehoseClient firehosePublisher) {
        this.firehosePublisher = firehosePublisher;
    }

    public void publish(String streamName, Object record) {
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .deliveryStreamName(streamName)
                .record(toRecord(record))
                .build();
        firehosePublisher.putRecord(putRecordRequest);

    }

    public void publishBatch(String streamName, List<Object> records) {
        PutRecordBatchRequest putRecordBatchRequest = PutRecordBatchRequest.builder()
                .deliveryStreamName(streamName)
                .records(records.stream()
                        .map(KinesisRecordPublisher::toRecord)
                        .toList())
                .build();
        firehosePublisher.putRecordBatch(putRecordBatchRequest);
    }

    private static Record toRecord(Object record) {
        return Record.builder()
                .data(SdkBytes.fromByteArray(record.toString().getBytes()))
                .build();
    }
}

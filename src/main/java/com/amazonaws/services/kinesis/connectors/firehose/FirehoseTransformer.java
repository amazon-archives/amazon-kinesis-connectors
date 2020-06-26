package com.amazonaws.services.kinesis.connectors.firehose;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase;
import com.amazonaws.services.kinesisfirehose.model.Record;

import java.io.IOException;

/**
 * Transformation class to be used with the FirehoseEmitter
 *
 * @param <T>
 */
public abstract class FirehoseTransformer<T> implements ITransformerBase<T, Record> {

    @Override
    public abstract Record fromClass(T record) throws IOException;

}

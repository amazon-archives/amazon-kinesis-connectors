package com.amazonaws.services.kinesis.connectors.s3;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;

public class S3ZipEmitter extends S3Emitter {
    private static final Log LOG = LogFactory.getLog(S3ZipEmitter.class);

    public S3ZipEmitter(KinesisConnectorConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected String getS3FileName(String firstSeq, String lastSeq) {
        return firstSeq + "-" + lastSeq + ".zip";
    }

    @Override
    public List<byte[]> emit(final UnmodifiableBuffer<byte[]> buffer) throws IOException {

        ArrayList<byte[]> compressedRecords = new ArrayList<byte[]>();

        List<byte[]> records = buffer.getRecords();

        // Write all of the records to a compressed output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (byte[] record : records) {
            try {
                baos.write(record);
            } catch (Exception e) {
                LOG.error("Error writing record to output stream. Failing this emit attempt. Record: "
                                + Arrays.toString(record),
                        e);
                return buffer.getRecords();
            }
        }

        // Get the Amazon S3 filename
        String s3FileName = super.getS3FileName(buffer.getFirstSequenceNumber(), buffer.getLastSequenceNumber());

        compressedRecords.add(this.compress(s3FileName, baos.toByteArray()));

        UnmodifiableBuffer<byte[]> newBuffer = new UnmodifiableBuffer<byte[]>(buffer, compressedRecords);

        return super.emit(newBuffer);
    }

    private byte[] compress(String filename, byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);
        ZipEntry entry = new ZipEntry(filename);
        entry.setSize(data.length);
        zos.putNextEntry(entry);
        zos.write(data);
        zos.closeEntry();
        zos.close();
        return baos.toByteArray();
    }

}

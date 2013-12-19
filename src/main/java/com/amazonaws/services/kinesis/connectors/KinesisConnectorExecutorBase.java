package com.amazonaws.services.kinesis.connectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

public abstract class KinesisConnectorExecutorBase<T,U> implements Runnable {
    private static final Log LOG = LogFactory.getLog(KinesisConnectorExecutorBase.class);

    // Amazon Kinesis Client Library worker to process records
    protected Worker worker;

    /**
     * Initialize the Amazon Kinesis Client Library configuration and worker
     * 
     * @param kinesisConnectorConfiguration Kinesis connector configuration
     */
    protected void initialize(KinesisConnectorConfiguration kinesisConnectorConfiguration) {
        initialize(kinesisConnectorConfiguration, null);
    }

    /**
     * Initialize the Amazon Kinesis Client Library configuration and worker with metrics factory
     * 
     * @param kinesisConnectorConfiguration Kinesis connector configuration
     * @param metricFactory would be used to emit metrics in Amazon Kinesis Client Library
     */
    protected void initialize(KinesisConnectorConfiguration kinesisConnectorConfiguration, IMetricsFactory metricFactory) {
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(kinesisConnectorConfiguration.APP_NAME,
                        kinesisConnectorConfiguration.KINESIS_INPUT_STREAM,
                        kinesisConnectorConfiguration.AWS_CREDENTIALS_PROVIDER,
                        kinesisConnectorConfiguration.WORKER_ID).withKinesisEndpoint(kinesisConnectorConfiguration.KINESIS_ENDPOINT)
                        .withFailoverTimeMillis(kinesisConnectorConfiguration.FAILOVER_TIME)
                        .withMaxRecords(kinesisConnectorConfiguration.MAX_RECORDS)
                        .withIdleTimeBetweenReadsInMillis(kinesisConnectorConfiguration.IDLE_TIME_BETWEEN_READS)
                        .withCallProcessRecordsEvenForEmptyRecordList(kinesisConnectorConfiguration.CALL_PROCESS_RECORDS_EVEN_FOR_EMPTY_LIST)
                        .withCleanupLeasesUponShardCompletion(kinesisConnectorConfiguration.CLEANUP_TERMINATED_SHARDS_BEFORE_EXPIRY)
                        .withParentShardPollIntervalMillis(kinesisConnectorConfiguration.PARENT_SHARD_POLL_INTERVAL)
                        .withShardSyncIntervalMillis(kinesisConnectorConfiguration.SHARD_SYNC_INTERVAL)
                        .withTaskBackoffTimeMillis(kinesisConnectorConfiguration.BACKOFF_INTERVAL)
                        .withMetricsBufferTimeMillis(kinesisConnectorConfiguration.CLOUDWATCH_BUFFER_TIME)
                        .withMetricsMaxQueueSize(kinesisConnectorConfiguration.CLOUDWATCH_MAX_QUEUE_SIZE)
                        .withUserAgent(kinesisConnectorConfiguration.APP_NAME + ","
                                + KinesisConnectorConfiguration.KINESIS_CONNECTOR_USER_AGENT);

        // If a metrics factory was specified, use it.
        if (metricFactory != null) {
            worker =
                    new Worker(getKinesisConnectorRecordProcessorFactory(),
                            kinesisClientLibConfiguration,
                            metricFactory);
        } else {
            worker = new Worker(getKinesisConnectorRecordProcessorFactory(), kinesisClientLibConfiguration);
        }
        LOG.info(getClass().getSimpleName() + " worker created");
    }

    @Override
    public void run() {
        if (worker != null) {
            // Start Kinesis client library worker to process records
            try {
                LOG.info("Starting worker in " + getClass().getSimpleName());
                worker.run();
            } catch (Throwable t) {
                LOG.error(t);
                throw t;
            }
        } else {
            throw new RuntimeException("Initialize must be called before run.");
        }
    }

    /**
     * This method returns a {@link KinesisConnectorRecordProcessorFactory} that contains the
     * appropriate {@link IKinesisConnectorPipeline} for the Kinesis Enabled Application
     * 
     * @return a {@link KinesisConnectorRecordProcessorFactory} that contains the appropriate
     *         {@link IKinesisConnectorPipeline} for the Kinesis Enabled Application
     */
    public abstract KinesisConnectorRecordProcessorFactory<T, U> getKinesisConnectorRecordProcessorFactory();
}

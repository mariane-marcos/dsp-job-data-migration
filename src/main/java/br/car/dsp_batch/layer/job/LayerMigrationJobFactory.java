package br.car.dsp_batch.layer.job;

import br.car.dsp_batch.batch.config.ChangeDecider;
import br.car.dsp_batch.batch.config.ParallelizationConfig;
import br.car.dsp_batch.batch.listener.GeoCacheUpdateListener;
import br.car.dsp_batch.batch.listener.ParallelizationMonitorListener;
import br.car.dsp_batch.layer.config.LayerConfig;
import br.car.dsp_batch.layer.ddl.LayerTableDdlBuilder;
import br.car.dsp_batch.layer.dto.LayerFeatureRecord;
import br.car.dsp_batch.layer.introspection.SchemaIntrospectionService;
import br.car.dsp_batch.layer.metadata.LayerMetadataRegistry;
import br.car.dsp_batch.layer.partitioner.DeferredLayerPartitioner;
import br.car.dsp_batch.geofile.GeoFileRegenerationFlagListener;
import br.car.dsp_batch.sync.SyncWatermarkCommitListener;
import br.car.dsp_batch.sync.WatermarkChangeDetectionEngine;
import br.car.dsp_batch.sync.SyncStateRepository;
import br.car.dsp_batch.layer.tasklet.LayerChangeDetectionTasklet;
import br.car.dsp_batch.layer.tasklet.LayerTableSetupTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Builds dynamic Spring Batch jobs for each configured geographic layer.
 */
@Slf4j
@Component
public class LayerMigrationJobFactory {

    public static final String JOB_NAME_PREFIX = "layerMigrationJob_";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource sourceDataSource;
    private final DataSource geoTargetDataSource;
    private final SchemaIntrospectionService introspectionService;
    private final LayerTableDdlBuilder ddlBuilder;
    private final LayerMetadataRegistry registry;
    private final WatermarkChangeDetectionEngine changeDetectionEngine;
    private final SyncStateRepository syncStateRepository;
    private final SyncWatermarkCommitListener watermarkCommitListener;
    private final ChangeDecider changeDecider;
    private final ParallelizationConfig parallelizationConfig;
    private final ParallelizationMonitorListener parallelizationMonitorListener;
    private final ItemReader<LayerFeatureRecord> layerFeatureReader;
    private final ItemWriter<LayerFeatureRecord> layerFeatureWriter;
    private final GeoFileRegenerationFlagListener geoFileRegenerationFlagListener;
    private final GeoCacheUpdateListener geoCacheUpdateListener;

    public LayerMigrationJobFactory(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("sourceDataSource") DataSource sourceDataSource,
            @Qualifier("geoTargetDataSource") DataSource geoTargetDataSource,
            SchemaIntrospectionService introspectionService,
            LayerTableDdlBuilder ddlBuilder,
            LayerMetadataRegistry registry,
            WatermarkChangeDetectionEngine changeDetectionEngine,
            SyncStateRepository syncStateRepository,
            SyncWatermarkCommitListener watermarkCommitListener,
            ChangeDecider changeDecider,
            ParallelizationConfig parallelizationConfig,
            ParallelizationMonitorListener parallelizationMonitorListener,
            @Qualifier("layerFeatureReader") ItemReader<LayerFeatureRecord> layerFeatureReader,
            @Qualifier("layerFeatureWriter") ItemWriter<LayerFeatureRecord> layerFeatureWriter,
            GeoFileRegenerationFlagListener geoFileRegenerationFlagListener,
            GeoCacheUpdateListener geoCacheUpdateListener) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.sourceDataSource = sourceDataSource;
        this.geoTargetDataSource = geoTargetDataSource;
        this.introspectionService = introspectionService;
        this.ddlBuilder = ddlBuilder;
        this.registry = registry;
        this.changeDetectionEngine = changeDetectionEngine;
        this.syncStateRepository = syncStateRepository;
        this.watermarkCommitListener = watermarkCommitListener;
        this.changeDecider = changeDecider;
        this.parallelizationConfig = parallelizationConfig;
        this.parallelizationMonitorListener = parallelizationMonitorListener;
        this.layerFeatureReader = layerFeatureReader;
        this.layerFeatureWriter = layerFeatureWriter;
        this.geoFileRegenerationFlagListener = geoFileRegenerationFlagListener;
        this.geoCacheUpdateListener = geoCacheUpdateListener;
    }

    public Job createJob(LayerConfig config) {
        String jobName = resolveJobName(config);
        String namePrefix = "layer_" + config.resolveKey();

        Step setupStep = buildSetupStep(namePrefix, config, jobName);
        Step changeDetectionStep = buildChangeDetectionStep(namePrefix, config);
        Step workerStep = buildWorkerStep(namePrefix, jobName);
        Step masterStep = buildMasterStep(namePrefix, jobName, workerStep, config);

        return new JobBuilder(jobName, jobRepository)
                .listener(watermarkCommitListener)
                .listener(geoFileRegenerationFlagListener)
                .listener(geoCacheUpdateListener)
                .start(setupStep)
                .next(changeDetectionStep)
                .next(changeDecider)
                .on("PROCESS").to(masterStep)
                .from(changeDecider).on("SKIP").end()
                .from(changeDecider).on("*").end()
                .end()
                .build();
    }

    public static String resolveJobName(LayerConfig config) {
        return JOB_NAME_PREFIX + config.resolveKey();
    }

    private Step buildSetupStep(String namePrefix, LayerConfig config, String jobName) {
        JdbcTemplate sourceJdbc = new JdbcTemplate(sourceDataSource);
        JdbcTemplate geoTargetJdbc = new JdbcTemplate(geoTargetDataSource);

        return new StepBuilder(namePrefix + "SetupStep", jobRepository)
                .tasklet(new LayerTableSetupTasklet(
                        sourceJdbc,
                        geoTargetJdbc,
                        introspectionService,
                        ddlBuilder,
                        registry,
                        config,
                        jobName
                ), transactionManager)
                .build();
    }

    private Step buildChangeDetectionStep(String namePrefix, LayerConfig config) {
        JdbcTemplate sourceJdbc = new JdbcTemplate(sourceDataSource);
        JdbcTemplate geoTargetJdbc = new JdbcTemplate(geoTargetDataSource);

        return new StepBuilder(namePrefix + "ChangeDetectionStep", jobRepository)
                .tasklet(new LayerChangeDetectionTasklet(
                        sourceJdbc,
                        geoTargetJdbc,
                        changeDetectionEngine,
                        registry,
                        config.resolveKey()
                ), transactionManager)
                .build();
    }

    private Step buildWorkerStep(String namePrefix, String jobName) {
        ParallelizationConfig.ParallelizationSettings settings =
                parallelizationConfig.getJobSettings(jobName);

        return new StepBuilder(namePrefix + "WorkerStep", jobRepository)
                .<LayerFeatureRecord, LayerFeatureRecord>chunk(settings.getChunkSize(), transactionManager)
                .reader(layerFeatureReader)
                .writer(layerFeatureWriter)
                .listener(parallelizationMonitorListener)
                .build();
    }

    private Step buildMasterStep(String namePrefix,
                                 String jobName,
                                 Step workerStep,
                                 LayerConfig config) {
        ParallelizationConfig.ParallelizationSettings settings =
                parallelizationConfig.getJobSettings(jobName);

        if (!settings.isEnabled()) {
            log.warn("{} - Partitioning disabled.", jobName);
            return workerStep;
        }

        Partitioner partitioner = new DeferredLayerPartitioner(
                registry, config.resolveKey(), sourceDataSource, syncStateRepository);
        TaskExecutor taskExecutor = createTaskExecutor(jobName, settings);

        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setStep(workerStep);
        partitionHandler.setTaskExecutor(taskExecutor);
        partitionHandler.setGridSize(settings.getThreadPoolSize());

        log.info("{} - Master step with {} partitions.", jobName, settings.getThreadPoolSize());

        return new StepBuilder(namePrefix + "MasterStep", jobRepository)
                .partitioner(workerStep.getName(), partitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    private TaskExecutor createTaskExecutor(String jobName,
                                            ParallelizationConfig.ParallelizationSettings settings) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int poolSize = settings.isEnabled() ? settings.getThreadPoolSize() : 1;
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setQueueCapacity(settings.getQueueCapacity());
        executor.setThreadNamePrefix(jobName + "-");
        executor.initialize();
        return executor;
    }
}

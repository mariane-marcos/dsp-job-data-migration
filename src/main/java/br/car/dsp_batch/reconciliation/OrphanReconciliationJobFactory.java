package br.car.dsp_batch.reconciliation;

import br.car.dsp_batch.batch.listener.GeoCacheUpdateListener;
import br.car.dsp_batch.geofile.GeoFileRegenerationFlagListener;
import br.car.dsp_batch.sync.CycleAffectedExtent;
import br.car.dsp_batch.reconciliation.partitioner.GeoTargetReconciliationPartitioner;
import br.car.dsp_batch.reconciliation.tasklet.OrphanReconciliationWorkerTasklet;
import br.car.dsp_batch.reconciliation.tasklet.ReconciliationSetupTasklet;
import br.car.dsp_batch.sync.WatermarkTableSpec;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Component
public class OrphanReconciliationJobFactory {

    private final ReconciliationProperties properties;
    private final ReconciliationEmptyTargetDecider emptyTargetDecider;
    private final ReconciliationCommitListener reconciliationCommitListener;
    private final GeoFileRegenerationFlagListener geoFileRegenerationFlagListener;
    private final GeoCacheUpdateListener geoCacheUpdateListener;
    private final CycleAffectedExtent cycleAffectedExtent;
    private final OrphanReconciliationService reconciliationService;
    private final OrphanOriginProbe originProbe;
    private final TaskExecutor reconciliationTaskExecutor;

    public OrphanReconciliationJobFactory(
            ReconciliationProperties properties,
            ReconciliationEmptyTargetDecider emptyTargetDecider,
            ReconciliationCommitListener reconciliationCommitListener,
            GeoFileRegenerationFlagListener geoFileRegenerationFlagListener,
            GeoCacheUpdateListener geoCacheUpdateListener,
            CycleAffectedExtent cycleAffectedExtent,
            OrphanReconciliationService reconciliationService,
            OrphanOriginProbe originProbe,
            @org.springframework.beans.factory.annotation.Qualifier("reconciliationTaskExecutor")
            TaskExecutor reconciliationTaskExecutor) {
        this.properties = properties;
        this.emptyTargetDecider = emptyTargetDecider;
        this.reconciliationCommitListener = reconciliationCommitListener;
        this.geoFileRegenerationFlagListener = geoFileRegenerationFlagListener;
        this.geoCacheUpdateListener = geoCacheUpdateListener;
        this.cycleAffectedExtent = cycleAffectedExtent;
        this.reconciliationService = reconciliationService;
        this.originProbe = originProbe;
        this.reconciliationTaskExecutor = reconciliationTaskExecutor;
    }

    public Job buildJob(JobRepository jobRepository,
                        PlatformTransactionManager transactionManager,
                        DataSource sourceDataSource,
                        DataSource businessTargetDataSource,
                        DataSource geoTargetDataSource,
                        String jobName,
                        String stepPrefix,
                        WatermarkTableSpec spec) {
        Step setupStep = buildSetupStep(
                jobRepository, transactionManager, geoTargetDataSource, stepPrefix, spec);
        Step masterStep = buildMasterStep(
                jobRepository,
                transactionManager,
                sourceDataSource,
                businessTargetDataSource,
                geoTargetDataSource,
                jobName,
                stepPrefix,
                spec);

        return new JobBuilder(jobName, jobRepository)
                .start(setupStep)
                .next(emptyTargetDecider)
                .on(ReconciliationEmptyTargetDecider.PROCESS).to(masterStep)
                .from(emptyTargetDecider).on(ReconciliationEmptyTargetDecider.SKIP).end()
                .from(emptyTargetDecider).on("*").end()
                .end()
                .listener(reconciliationCommitListener)
                .listener(geoFileRegenerationFlagListener)
                .listener(geoCacheUpdateListener)
                .build();
    }

    public Step buildSetupStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager,
                               DataSource geoTargetDataSource,
                               String stepPrefix,
                               WatermarkTableSpec spec) {
        Tasklet tasklet = new ReconciliationSetupTasklet(
                new JdbcTemplate(geoTargetDataSource),
                spec,
                properties,
                cycleAffectedExtent);
        return new StepBuilder(stepPrefix + "ReconciliationSetupStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    public Step buildMasterStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                DataSource sourceDataSource,
                                DataSource businessTargetDataSource,
                                DataSource geoTargetDataSource,
                                String jobName,
                                String stepPrefix,
                                WatermarkTableSpec spec) {
        Step workerStep = buildWorkerStep(
                jobRepository,
                transactionManager,
                sourceDataSource,
                businessTargetDataSource,
                geoTargetDataSource,
                stepPrefix,
                spec);
        Partitioner partitioner = new GeoTargetReconciliationPartitioner(
                new JdbcTemplate(geoTargetDataSource), spec);

        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setGridSize(properties.getGridSize());
        handler.setTaskExecutor(reconciliationTaskExecutor);

        return new StepBuilder(stepPrefix + "ReconciliationMasterStep", jobRepository)
                .partitioner(workerStep.getName(), partitioner)
                .partitionHandler(handler)
                .build();
    }

    public Step buildWorkerStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                DataSource sourceDataSource,
                                DataSource businessTargetDataSource,
                                DataSource geoTargetDataSource,
                                String stepPrefix,
                                WatermarkTableSpec spec) {
        return new StepBuilder(stepPrefix + "ReconciliationWorkerStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    ExecutionContext stepContext = chunkContext.getStepContext()
                            .getStepExecution()
                            .getExecutionContext();
                    return buildWorkerTasklet(
                            sourceDataSource,
                            businessTargetDataSource,
                            geoTargetDataSource,
                            spec,
                            stepContext).execute(contribution, chunkContext);
                }, transactionManager)
                .build();
    }

    private Tasklet buildWorkerTasklet(DataSource sourceDataSource,
                                       DataSource businessTargetDataSource,
                                       DataSource geoTargetDataSource,
                                       WatermarkTableSpec spec,
                                       ExecutionContext stepContext) {
        return new OrphanReconciliationWorkerTasklet(
                new JdbcTemplate(sourceDataSource),
                businessTargetDataSource != null ? new JdbcTemplate(businessTargetDataSource) : null,
                new JdbcTemplate(geoTargetDataSource),
                spec,
                reconciliationService,
                originProbe,
                properties,
                OrphanReconciliationWorkerTasklet.rangeStart(stepContext),
                OrphanReconciliationWorkerTasklet.rangeEnd(stepContext));
    }
}

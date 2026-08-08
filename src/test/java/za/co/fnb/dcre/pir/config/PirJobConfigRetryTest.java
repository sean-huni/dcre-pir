package za.co.fnb.dcre.pir.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import za.co.fnb.dcre.pir.service.InitialResponseTasklet;
import za.co.fnb.dcre.platform.batch.HeartbeatWriter;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Registration proof for the shared CRDB 40001 retry (SCRUM-42): the REAL
 * responseStep built by PirJobConfig must swallow a commit-time
 * TransientDataAccessException (how CockroachDB serialization aborts surface)
 * and re-run the respond tasklet in a fresh transaction. PIR writes are small
 * (response ledger + idempotent staging) but run concurrent with the fleet's
 * heavy writers. Handler semantics themselves are proven in platform-batch.
 */
class PirJobConfigRetryTest {

    /** Fails the first {@code failures} COMMITS the way JdbcTransactionManager surfaces a CRDB 40001. */
    static final class CommitFailingTxManager extends ResourcelessTransactionManager {

        private final int failures;
        private int commits;

        CommitFailingTxManager(final int failures) {
            this.failures = failures;
        }

        @Override
        protected void doCommit(final DefaultTransactionStatus status) {
            if (++commits <= failures) {
                throw new CannotAcquireLockException(
                        "JDBC commit; ERROR: restart transaction: TransactionRetryWithProtoRefreshError:"
                                + " RETRY_SERIALIZABLE");
            }
            super.doCommit(status);
        }
    }

    @Test
    void respondStepRetriesCommitTime40001Aborts() throws Exception {
        final var executions = new AtomicInteger();
        final InitialResponseTasklet tasklet = new InitialResponseTasklet(null) {
            @Override
            public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) {
                executions.incrementAndGet();
                return RepeatStatus.FINISHED;
            }
        };
        final var repo = new ResourcelessJobRepository();
        // SCRUM-88: the job now also registers a HeartbeatWriter listener; this unit test drives
        // the STEP directly (never the job lifecycle), so a no-op writer (null datasource, no
        // JOB_NAME) satisfies the signature without touching the retry behaviour under test.
        final Job job = new PirJobConfig().pirJob(repo, new CommitFailingTxManager(2), tasklet,
                new HeartbeatWriter(null, null, null), "build/test-exchange");
        final Step step = ((StepLocator) job).getStep("responseStep");

        final JobInstance instance = repo.createJobInstance("retryWiringJob", new JobParameters());
        final JobExecution jobExecution = repo.createJobExecution(instance, new JobParameters(), new ExecutionContext());
        final StepExecution stepExecution = repo.createStepExecution("responseStep", jobExecution);
        step.execute(stepExecution);

        assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus(),
                "commit-time 40001 aborts must be retried on the respond step, not fail it");
        assertEquals(3, executions.get(), "respond tasklet re-runs in a fresh tx per aborted commit");
    }
}

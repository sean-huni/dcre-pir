package za.co.fnb.dcre.pir.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.pir.service.InitialResponseTasklet;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.HeartbeatWriter;
import za.co.fnb.dcre.platform.batch.OutcomeSeamListener;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;

@Configuration
public class PirJobConfig {

    @Bean
    public Job pirJob(final JobRepository repo, final PlatformTransactionManager tx,
                      final InitialResponseTasklet tasklet, final HeartbeatWriter heartbeatWriter,
                      @Value("${dcre.exchange-root}") final String exchangeRoot) {
        // SCRUM-42: the respond step WRITES the response ledger + idempotent staging concurrent
        // with the fleet's heavy writers; CRDB 40001 commit-time aborts are normal under
        // contention and are retried in a fresh tx by the shared handler (retry, never skip).
        Step responseStep = new StepBuilder("responseStep", repo)
                .tasklet(tasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("PIR"))
                .build();
        // SCRUM-58: shared platform-batch seam listener replaces the inline record; COMPLETED gate
        // and constant BUSINESS_ACCEPTED verdict unchanged, the dev fallback name upgrades to the
        // self-describing local-pir-<executionId>. pir_response is captured on the respond path
        // (per-arrival), not here, so this listener carries no persistence hook.
        // SCRUM-88 (M12): the HeartbeatWriter is a JobExecutionListener but Batch 6 does not
        // auto-apply listener beans; register it explicitly so it stamps agt_ops liveness while
        // this job runs, chained after the outcome seam listener.
        return new JobBuilder("pirJob", repo)
                .listener(new OutcomeSeamListener("pir", exchangeRoot, execution -> "BUSINESS_ACCEPTED"))
                .listener(heartbeatWriter)
                .start(responseStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "PIR_BATCH_", 60);
    }
}

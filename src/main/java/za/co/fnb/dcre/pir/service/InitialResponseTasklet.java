package za.co.fnb.dcre.pir.service;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** Thin entry adapter (3-tier, configuration.md point 21): params in, one service call, status out. */
@Component
public class InitialResponseTasklet implements Tasklet {

    private final InitialResponseService service;

    public InitialResponseTasklet(final InitialResponseService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext)
            throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        InitialResponseService.Result result = service.respond(
                UUID.fromString((String) params.get("arrival.id")),
                (String) params.get("route.id"),
                (String) params.get("fatal.reason"),
                (String) params.get("client.token"),
                (String) params.get("msg.id"),
                (String) params.get("outcome.hint"));
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putString("responseFile", result.responseFile().toString());
        if (!result.written()) {
            contribution.setExitStatus(new ExitStatus("COMPLETED", "response already existed: restart no-op"));
        }
        return RepeatStatus.FINISHED;
    }
}

package za.co.fnb.dcre.pir;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class PirJobTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    Job pirJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    void seed(UUID arrival, String msgId, int total, List<String> failures) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35), tx_count INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, tx_count) VALUES (?,?,?,?)",
                arrival, msgId, "FNBRF01", total);
        for (int i = 0; i < total; i++) {
            String outcome = i < failures.size() ? failures.get(i) : "PASS";
            jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                    arrival, i + 1, outcome);
        }
    }

    @Test
    void ackWithRejectDetailsThenRestartNoOp() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFTEST" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 5, List.of("FAIL_ACCOUNT_NOT_ACTIVE", "FAIL_EXCEEDS_RF_BALANCE"));

        JobExecution run = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        Path file = Path.of(run.getExecutionContext().getString("responseFile"));
        List<String> lines = Files.readAllLines(file);
        assertEquals("ACK|FNBRF01|" + msgId + "|3/5|ACCEPTED_BY_DCRE", lines.get(0));
        assertEquals(3, lines.size());
        assertTrue(lines.get(1).startsWith("REJ|1|FAIL_ACCOUNT_NOT_ACTIVE"));

        // restart no-op: second run (new identity to dodge JobInstance reuse) must not rewrite
        Files.writeString(file, String.join("\n", lines)); // ensure content fixed
        JobExecution rerun = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .addString("attempt", "2", true).toJobParameters());
        assertEquals(BatchStatus.COMPLETED, rerun.getStatus());
        assertEquals(lines, Files.readAllLines(file), "existing response is never overwritten (R-05)");
    }

    @Test
    void fileFatalArrivalGetsNack() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFFATAL" + arrival.toString().substring(0, 4);
        seed(arrival, msgId, 3, List.of());
        jdbc.update("DELETE FROM validation_log WHERE arrival_id=?", arrival); // fatal: no verdicts

        JobExecution run = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .addString("fatal.reason", "V1 layout fails closed in production (A-2)", false)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        List<String> lines = Files.readAllLines(Path.of(run.getExecutionContext().getString("responseFile")));
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).startsWith("NACK|FNBRF01|" + msgId + "|0/3|V1 layout"));
    }

    @Test
    void launchWithoutRouteIdFailsClosedAndWritesNothing() throws Exception {
        // A-45 seam-level safety: an AGT launch missing route.id must FAIL the job,
        // never stage a response under a collision-prone identity
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFNOROUTE" + arrival.toString().substring(0, 4);
        seed(arrival, msgId, 2, List.of());

        JobExecution run = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true).toJobParameters());

        assertEquals(BatchStatus.FAILED, run.getStatus());
        assertEquals(ExitStatus.FAILED.getExitCode(), run.getExitStatus().getExitCode());
        assertFalse(run.getExecutionContext().containsKey("responseFile"),
                "a failed launch must not advertise a response file");
        // SCRUM-42: responses land under the per-client <base>/onhost-resp/out tree
        Path respDir = Files.createDirectories(Path.of("build/test-exchange/fnbrf01/onhost-resp/out"));
        try (var responses = Files.list(respDir)) {
            assertTrue(responses.noneMatch(p -> p.getFileName().toString().contains(msgId)),
                    "no response file may be staged without the route identity");
        }
    }

    @Test
    void launchWithUnconfiguredClientFailsClosedAndWritesNothing() throws Exception {
        // SCRUM-42 / A-42: a headerless arrival with no client.token falls back to "UNKNOWN",
        // which has no configured exchange dir. The layout fails closed so the JOB FAILS rather
        // than staging a NACK under a shared/wrong dir (no legacy global fallback on 2.0.1).
        UUID arrival = UUID.randomUUID();   // NO tx_header seeded and NO client.token param
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35), tx_count INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");

        JobExecution run = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .addString("fatal.reason", "no header persisted (A-42)", false)
                .toJobParameters());

        assertEquals(BatchStatus.FAILED, run.getStatus());
        assertFalse(run.getExecutionContext().containsKey("responseFile"),
                "a fail-closed launch must not advertise a response file");
        // scope to THIS launch's unique arrivalId (the UNKNOWN fallback names the file
        // UNKNOWN_<arrivalId>_...), so the assertion is immune to unrelated artifacts
        Path root = Path.of("build/test-exchange");
        if (Files.exists(root)) {
            try (var walk = Files.walk(root)) {
                assertTrue(walk.noneMatch(p -> p.getFileName().toString().contains(arrival.toString())),
                        "no response file may be staged for an unconfigured (UNKNOWN) client");
            }
        }
    }
}

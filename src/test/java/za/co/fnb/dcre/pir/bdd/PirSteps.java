package za.co.fnb.dcre.pir.bdd;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step definitions for the initial response writer. Seeding mirrors
 * PirJobTest: tx_header + validation_log rows stand in for the PRR/PTV
 * upstream writers (R-04 single-writer seams).
 */
public class PirSteps {

    @Autowired
    Job pirJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    UUID arrival;
    String msgId;
    JobExecution execution;
    List<String> firstRunContent;

    @Given("an arrival of {int} payment requests that all passed validation")
    public void allPassArrival(int total) {
        seed(total, List.of());
    }

    @Given("an arrival of {int} payment requests where the first records failed validation as:")
    public void mixedArrival(int total, DataTable table) {
        seed(total, table.asMaps().stream().map(row -> row.get("outcome")).toList());
    }

    @Given("a file-fatal arrival declaring {int} payment requests with no verdicts recorded")
    public void fatalArrival(int total) {
        seed(total, List.of());
        jdbc.update("DELETE FROM validation_log WHERE arrival_id=?", arrival);
    }

    @When("the initial response job runs")
    public void jobRuns() throws Exception {
        execution = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
    }

    @When("the initial response job runs citing the fatal reason {string}")
    public void jobRunsWithFatalReason(String reason) throws Exception {
        execution = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .addString("fatal.reason", reason, false)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
    }

    @When("the initial response job runs again for the same arrival")
    public void jobRunsAgain() throws Exception {
        firstRunContent = responseLines();
        // new attempt parameter: a fresh JobInstance re-emitting the same
        // arrival exercises the StagedWrite restart no-op, not instance refusal
        execution = jobOperator.start(pirJob, new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("route.id", "onhost-req-pay", false)
                .addString("attempt", "2", true)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
    }

    @Then("the response file acknowledges {int} of {int} transactions")
    public void responseAcknowledges(int accepted, int total) throws Exception {
        assertEquals("ACK|FNBRF01|" + msgId + "|" + accepted + "/" + total + "|ACCEPTED_BY_DCRE",
                responseLines().get(0));
    }

    @Then("the response file carries no rejection details")
    public void noRejectionDetails() throws Exception {
        assertEquals(1, responseLines().size(), "an all-PASS arrival yields the ACK line only");
    }

    @Then("the response file lists the rejections:")
    public void listsRejections(DataTable table) throws Exception {
        List<Map<String, String>> expected = table.asMaps();
        List<String> lines = responseLines();
        assertEquals(1 + expected.size(), lines.size(), "ACK line plus one REJ line per rejection");
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("REJ|" + expected.get(i).get("sequence") + "|" + expected.get(i).get("outcome"),
                    lines.get(i + 1));
        }
    }

    @Then("the response file is a NACK for {int} transactions citing {string}")
    public void responseIsNack(int total, String reason) throws Exception {
        List<String> lines = responseLines();
        assertEquals(1, lines.size(), "a NACK carries a single line");
        assertEquals("NACK|FNBRF01|" + msgId + "|0/" + total + "|" + reason, lines.get(0));
    }

    @Then("the response filename is recorded in pir_response as an ACK of {int} of {int}")
    public void ledgerRecordsResponse(int accepted, int total) {
        // SCRUM-58: after a COMPLETED job the ledger is the system of record for the filename.
        Map<String, Object> row = jdbc.queryForMap("SELECT outcome, file_name, accepted_count,"
                + " total_count, written_at FROM pir_response WHERE arrival_id=?", arrival);
        assertEquals("ACK", row.get("outcome"));
        assertEquals(responseFile().getFileName().toString(), row.get("file_name"),
                "the ledger records the staged response basename");
        assertEquals(accepted, ((Number) row.get("accepted_count")).intValue());
        assertEquals(total, ((Number) row.get("total_count")).intValue());
        assertNotNull(row.get("written_at"), "written_at is stamped after the file write");
    }

    @Then("the response file on disk is unchanged")
    public void responseUnchanged() throws Exception {
        assertEquals(firstRunContent, responseLines(),
                "an existing response is never overwritten (R-05 StagedWrite no-op)");
        assertTrue(Files.notExists(responseFile().resolveSibling(responseFile().getFileName() + ".tmp")),
                "no staging leftovers");
    }

    void seed(int total, List<String> failures) {
        arrival = UUID.randomUUID();
        msgId = "DCRERFBDD" + arrival.toString().substring(0, 6);
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

    Path responseFile() {
        return Path.of(execution.getExecutionContext().getString("responseFile"));
    }

    List<String> responseLines() throws Exception {
        return Files.readAllLines(responseFile());
    }
}

package za.co.fnb.dcre.pir.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.pir.data.model.PirResponseEntity;
import za.co.fnb.dcre.pir.data.repo.PirResponseRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-58: pir_response is the write-ahead filename ledger and system of
 * record for every initial response. A prod supporter holding a response file
 * name resolves the arrival, client, route, outcome and per-file acceptance
 * ratio from this one table. Reads go through JdbcTemplate on purpose: the row
 * committed to the database is the contract, independent of the entity mapping.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class PirResponseCaptureIT {

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
    InitialResponseService service;

    @Autowired
    PirResponseRepo repo;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void upstreamTablesExist() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35), tx_count INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
    }

    @Test
    void nackFilenameIsQueryableWithItsReason() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFCAP" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 4, List.of("FAIL_ACCOUNT_NOT_ACTIVE", "PASS", "FAIL_DUPLICATE_TX", "PASS"));

        service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, "BUSINESS_FILE_REJECTED");

        Map<String, Object> row = ledgerRow(arrival);
        assertEquals("NACK", row.get("outcome"));
        assertEquals("FILE_REJECTED_BY_POLICY", row.get("reason"), "A-48: the NACK reason is now persisted");
        assertEquals("FNBRF01_" + msgId + "_onhost-req-pay_RESP.txt", row.get("file_name"));
        assertEquals(0, ((Number) row.get("accepted_count")).intValue());
        assertEquals(4, ((Number) row.get("total_count")).intValue());
        assertNotNull(row.get("written_at"), "written_at is stamped after the file write");
    }

    @Test
    void crossRouteTwinYieldsTwoRowsWithDistinctFilenames() throws Exception {
        // A-45: the same (client, msgId) arriving on two routes are DISTINCT arrivals.
        String msgId = "DCRERFTWIN" + UUID.randomUUID().toString().substring(0, 5);
        UUID req = UUID.randomUUID();
        UUID twin = UUID.randomUUID();
        seed(req, msgId, 2, List.of("PASS", "PASS"));
        seed(twin, msgId, 2, List.of("PASS", "PASS"));

        service.respond(req, "onhost-req-pay", null, "FNBRF01", msgId, null);
        service.respond(twin, "fint-resp-pay", null, "FNBRF01", msgId, null);

        assertEquals("FNBRF01_" + msgId + "_onhost-req-pay_RESP.txt", ledgerRow(req).get("file_name"));
        assertEquals("FNBRF01_" + msgId + "_fint-resp-pay_RESP.txt", ledgerRow(twin).get("file_name"));
        assertEquals(2L, jdbc.queryForObject(
                "SELECT count(*) FROM pir_response WHERE msg_id=?", Long.class, msgId));
    }

    @Test
    void killBetweenRowCommitAndStagedWriteResumesToOneStampedFileAndRow() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFKILL" + arrival.toString().substring(0, 5);
        seed(arrival, msgId, 5, List.of());
        String fileName = "FNBRF01_" + msgId + "_onhost-req-pay_RESP.txt";

        // Durable state after the write-ahead insert committed but BEFORE the file write.
        jdbc.update("INSERT INTO pir_response (arrival_id, client, msg_id, route_id, outcome,"
                + " file_name, accepted_count, total_count) VALUES (?,?,?,?,?,?,?,?)",
                arrival, "FNBRF01", msgId, "onhost-req-pay", "ACK", fileName, 5, 5);
        assertNull(ledgerRow(arrival).get("written_at"), "staged-not-written: written_at IS NULL");

        var resumed = service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, null);

        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM pir_response WHERE arrival_id=?",
                Long.class, arrival), "resume ON CONFLICT no-ops: exactly one row per arrival");
        assertNotNull(ledgerRow(arrival).get("written_at"), "resume stamps written_at");
        assertTrue(Files.exists(resumed.responseFile()), "resume writes exactly one file");
        assertEquals("ACK|FNBRF01|" + msgId + "|5/5|ACCEPTED_BY_DCRE",
                Files.readAllLines(resumed.responseFile()).get(0));
    }

    @Test
    void headerlessFallbackIsCapturedWithItsReason() throws Exception {
        UUID arrival = UUID.randomUUID();   // NO tx_header row: the A-42 headerless shape
        String msgId = "DCRERFA42" + arrival.toString().substring(0, 6);

        service.respond(arrival, "onhost-req-pay", "spine count 10 != declared 11", "FNBRF01", msgId, null);

        Map<String, Object> row = ledgerRow(arrival);
        assertEquals("NACK", row.get("outcome"));
        assertEquals("spine count 10 != declared 11", row.get("reason"),
                "A-48: the headerless NACK reason is persisted, not lost");
        assertEquals(0, ((Number) row.get("total_count")).intValue());
    }

    @Test
    void acceptedCountAndTotalCountMatchAPartialFile() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFPART" + arrival.toString().substring(0, 5);
        seed(arrival, msgId, 3, List.of("FAIL_EXCEEDS_RF_BALANCE", "PASS", "PASS"));

        service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, "BUSINESS_PARTIAL");

        Map<String, Object> row = ledgerRow(arrival);
        assertEquals("ACK", row.get("outcome"), "a partial file is still an ACK (accepted-by-DCRE)");
        assertEquals(2, ((Number) row.get("accepted_count")).intValue());
        assertEquals(3, ((Number) row.get("total_count")).intValue());
        assertNull(row.get("reason"), "a clean/partial ACK carries no NACK reason");

        // The aggregate read maps every ledger column back (proves the entity/repo contract).
        PirResponseEntity entity = repo.findByArrivalId(arrival).orElseThrow();
        assertEquals("ACK", entity.getOutcome());
        assertEquals(2, entity.getAcceptedCount());
        assertEquals(3, entity.getTotalCount());
        assertEquals("onhost-req-pay", entity.getRouteId());
        assertNotNull(entity.getWrittenAt());
    }

    @Test
    void overlongHeaderlessReasonStillNacksWithAClippedReason() throws Exception {
        // A free-text fatalReason can exceed reason VARCHAR(64); it must NOT fail the write-ahead
        // INSERT (which would turn a describable NACK into a hard job failure with no file).
        UUID arrival = UUID.randomUUID();   // headerless A-42 path
        String msgId = "DCRERFLONG" + arrival.toString().substring(0, 5);
        String longReason = "spine transaction count 1000 does not match declared control total 1001 in trailer";
        assertTrue(longReason.length() > 64, "fixture must exceed the column width");

        var result = service.respond(arrival, "onhost-req-pay", longReason, "FNBRF01", msgId, null);

        assertTrue(Files.exists(result.responseFile()), "an over-length reason must not fail the write-ahead");
        assertTrue(Files.readAllLines(result.responseFile()).get(0).endsWith("|" + longReason),
                "the response file line keeps the full reason");
        assertEquals(longReason.substring(0, 64), ledgerRow(arrival).get("reason"),
                "the ledger reason is clipped to VARCHAR(64)");
    }

    @Test
    void overlongClientIdentityFailsClosedWritingNeitherRowNorFile() {
        // M2: pir_response.client is VARCHAR(16) but its source tx_header.initg_pty is VARCHAR(35).
        // An over-length client identity must fail closed with a readable business message BEFORE the
        // write-ahead INSERT, not surface as a raw CRDB "value too long for type varchar(16)" that
        // kills the job. Identity is NEVER clipped (unlike reason); this only guards the column width.
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFWIDE" + arrival.toString().substring(0, 5);
        String overlongClient = "LONGINITIATINGPARTY01";   // 21 chars: > client(16), <= initg_pty(35)
        assertTrue(overlongClient.length() > 16 && overlongClient.length() <= 35,
                "fixture must exceed the client column width but fit initg_pty");
        // A CONFIGURED client (see dcre-exchange-layout.yml), so layout.resolve succeeds and the
        // failure can only come from the width guard, not from an unconfigured-client fail-closed.
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, tx_count) VALUES (?,?,?,?)",
                arrival, msgId, overlongClient, 2);
        jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                arrival, 1, "PASS");
        jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                arrival, 2, "PASS");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> service.respond(arrival, "onhost-req-pay", null, overlongClient, msgId, null),
                "an over-length client identity fails closed, not with a raw SQLException");
        assertTrue(ex.getMessage().contains("pir_response.client identity exceeds 16 chars"),
                "the fail-closed message names the offending column and width, not a SQL error");
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM pir_response WHERE arrival_id=?",
                Long.class, arrival), "an over-length client identity writes no ledger row");
    }

    @Test
    void failClosedUnknownClientWritesNeitherRowNorFile() {
        // Invariant: resolve BEFORE capture, so a fail-closed config error leaves no phantom row.
        UUID arrival = UUID.randomUUID();   // no header AND no client.token -> UNKNOWN, no exchange dir
        assertThrows(IllegalArgumentException.class,
                () -> service.respond(arrival, "onhost-req-pay", "spine truncated", null, null, null));
        assertEquals(0L, jdbc.queryForObject("SELECT count(*) FROM pir_response WHERE arrival_id=?",
                Long.class, arrival), "a fail-closed config error writes no ledger row");
    }

    @Test
    void killBetweenFileWriteAndStampResumesToStampWithoutRewriting() throws Exception {
        // The other kill class: row + file are present but written_at is still NULL. Resume must
        // stamp the existing row and NOT rewrite the file (R-05 restart no-op), zero duplicate rows.
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFSTMP" + arrival.toString().substring(0, 5);
        seed(arrival, msgId, 3, List.of());
        service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, null);
        jdbc.update("UPDATE pir_response SET written_at = NULL WHERE arrival_id=?", arrival);

        var resumed = service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, null);

        assertFalse(resumed.written(), "the existing file is a restart no-op, never a rewrite");
        assertEquals(1L, jdbc.queryForObject("SELECT count(*) FROM pir_response WHERE arrival_id=?",
                Long.class, arrival), "resume never duplicates the row");
        assertNotNull(ledgerRow(arrival).get("written_at"), "resume stamps the previously-unstamped row");
    }

    private Map<String, Object> ledgerRow(final UUID arrival) {
        return jdbc.queryForMap("SELECT outcome, reason, file_name, accepted_count, total_count, written_at"
                + " FROM pir_response WHERE arrival_id=?", arrival);
    }

    private void seed(UUID arrival, String msgId, int total, List<String> outcomes) {
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, tx_count) VALUES (?,?,?,?)",
                arrival, msgId, "FNBRF01", total);
        for (int i = 0; i < total; i++) {
            jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                    arrival, i + 1, i < outcomes.size() ? outcomes.get(i) : "PASS");
        }
    }
}

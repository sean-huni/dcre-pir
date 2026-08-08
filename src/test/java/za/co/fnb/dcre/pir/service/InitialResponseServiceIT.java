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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A-42: arrivals whose tx_header was never persisted (PRR BUSINESS_FILE_FATAL)
 * must still produce a NACK from the AGT-supplied job params instead of
 * crashing with NoSuchElementException. R-41: BUSINESS_FILE_REJECTED arrivals
 * (ALL_OR_NOTHING policy) produce a FILE_REJECTED_BY_POLICY NACK with REJ detail.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class InitialResponseServiceIT {

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
    JdbcTemplate jdbc;

    @BeforeEach
    void upstreamTablesExistAndFixedResponsesAreCleared() throws IOException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35), tx_count INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        // fixed msg ids reused across runs: clear so StagedWrite writes, not no-ops.
        // SCRUM-42: responses now land under the per-client <base>/onhost-resp/out tree.
        Files.deleteIfExists(Path.of("build/test-exchange/fnbrf01/onhost-resp/out/FNBRF01_DCRERF2026071313500102_onhost-req-pay_RESP.txt"));
        Files.deleteIfExists(Path.of("build/test-exchange/fnbrf01/onhost-resp/out/FNBRF01_DCRERF2026071313500103_onhost-req-pay_RESP.txt"));
    }

    @Test
    void headerlessArrivalStillProducesNack() throws Exception {
        UUID arrival = UUID.randomUUID();   // NO tx_header row seeded: the A-42 crash shape
        var result = service.respond(arrival, "onhost-req-pay", "spine count 10 != declared 11",
                "FNBRF01", "DCRERF2026071313500102", null);
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("NACK|FNBRF01|DCRERF2026071313500102|0/0|spine count 10 != declared 11", lines.get(0));
    }

    @Test
    void headerlessArrivalWithoutFatalReasonNacksWithNoHeaderLiteral() throws Exception {
        UUID arrival = UUID.randomUUID();   // no header, no fatal.reason param either
        var result = service.respond(arrival, "onhost-req-pay", null, "FNBRF01", "DCRERF2026071313500103", null);
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals(1, lines.size());
        assertEquals("NACK|FNBRF01|DCRERF2026071313500103|0/0|NO_HEADER", lines.get(0));
    }

    @Test
    void headerlessArrivalWithoutClientTokenFailsClosed() {
        // SCRUM-42 / A-42 on the 2.0.1 fleet: with no header AND no client.token, the client
        // falls back to "UNKNOWN", which has NO configured exchange dir. layout.resolve fails
        // closed (throws) so the job fails rather than staging a NACK under a shared/wrong dir.
        // AGT on the 2.0.1 fleet always passes client identity, so this is a config error, not a
        // normal case; there is deliberately NO legacy global fallback dir.
        UUID arrival = UUID.randomUUID();   // no tx_header row, no client.token param

        var ex = assertThrows(IllegalArgumentException.class,
                () -> service.respond(arrival, "onhost-req-pay", "spine truncated", null, null, null));
        assertTrue(ex.getMessage().contains("UNKNOWN"),
                "unconfigured client must fail closed: " + ex.getMessage());
    }

    @Test
    void businessFileRejectedArrivalNacksWithPolicyReasonAndRejDetail() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFPOL" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 4, List.of("FAIL_ACCOUNT_NOT_ACTIVE", "PASS", "FAIL_DUPLICATE_TX", "PASS"));

        var result = service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, "BUSINESS_FILE_REJECTED");
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("NACK|FNBRF01|" + msgId + "|0/4|FILE_REJECTED_BY_POLICY", lines.get(0));
        assertEquals(3, lines.size(), "NACK line plus one REJ line per non-PASS verdict");
        assertEquals("REJ|1|FAIL_ACCOUNT_NOT_ACTIVE", lines.get(1));
        assertEquals("REJ|3|FAIL_DUPLICATE_TX", lines.get(2));
    }

    @Test
    void sameClientAndMsgIdOnDifferentRoutesGetDistinctResponses() throws Exception {
        // A-45: (client, msgId) repeats across routes as DISTINCT arrivals. An twin-route
        // ACK must never satisfy the onhost-req-pay arrival's NACK as a restart no-op.
        UUID twinArrival = UUID.randomUUID();
        String msgId = "DCRERFA45" + twinArrival.toString().substring(0, 6);
        seed(twinArrival, msgId, 2, List.of("PASS", "PASS"));
        var twinResult = service.respond(twinArrival, "fint-resp-pay", null, "FNBRF01", msgId, null);
        assertTrue(twinResult.written());
        assertTrue(Files.readAllLines(twinResult.responseFile()).get(0).startsWith("ACK|"));

        UUID reqArrival = UUID.randomUUID();
        seed(reqArrival, msgId, 2, List.of("FAIL_DUPLICATE_TX", "PASS"));
        var reqResult = service.respond(reqArrival, "onhost-req-pay", null, "FNBRF01", msgId, "BUSINESS_FILE_REJECTED");

        assertTrue(reqResult.written(),
                "rejected onhost-req-pay arrival must get its own NACK, not a stale-ACK no-op on the twin file");
        assertNotEquals(twinResult.responseFile(), reqResult.responseFile(),
                "distinct arrivals (different routes) must never share a response file");
        assertEquals("FNBRF01_" + msgId + "_fint-resp-pay_RESP.txt",
                twinResult.responseFile().getFileName().toString(),
                "documented target shape: <client>_<msgId>_<route>_RESP.txt");
        assertEquals("FNBRF01_" + msgId + "_onhost-req-pay_RESP.txt",
                reqResult.responseFile().getFileName().toString(),
                "documented target shape: <client>_<msgId>_<route>_RESP.txt");
        assertEquals(Path.of("build/test-exchange/fnbrf01/onhost-resp/out"),
                reqResult.responseFile().getParent(),
                "SCRUM-42: response lands in the per-client <base>/onhost-resp/out dir");
        assertEquals("NACK|FNBRF01|" + msgId + "|0/2|FILE_REJECTED_BY_POLICY",
                Files.readAllLines(reqResult.responseFile()).get(0));
    }

    @Test
    void missingOrInvalidRouteFailsClosed() {
        // A-45 fail-closed: no fallback token, a missing route is a config error; the
        // whitelist [a-z0-9-]+ also blocks tokens that would escape onhost-resp as a path
        UUID arrival = UUID.randomUUID();
        for (String badRoute : new String[] {null, "  ", "../onhost-req-pay", "onhost-req-pay/../../etc",
                "ONHOST-REQ", "onhost_req"}) {
            var ex = assertThrows(IllegalArgumentException.class,
                    () -> service.respond(arrival, badRoute, null, "FNBRF01", "DCRERFA45MISS", null),
                    "route must be rejected: " + badRoute);
            assertEquals("arrival route missing or invalid: required for response identity (A-45)",
                    ex.getMessage());
        }
    }

    @Test
    void acceptedArrivalKeepsExistingAckBehavior() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFACK" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 3, List.of("FAIL_EXCEEDS_RF_BALANCE", "PASS", "PASS"));

        var result = service.respond(arrival, "onhost-req-pay", null, "FNBRF01", msgId, "BUSINESS_PARTIAL");
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("ACK|FNBRF01|" + msgId + "|2/3|ACCEPTED_BY_DCRE", lines.get(0));
        assertEquals("REJ|1|FAIL_EXCEEDS_RF_BALANCE", lines.get(1));
    }

    void seed(UUID arrival, String msgId, int total, List<String> outcomes) {
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, tx_count) VALUES (?,?,?,?)",
                arrival, msgId, "FNBRF01", total);
        for (int i = 0; i < total; i++) {
            jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                    arrival, i + 1, outcomes.get(i));
        }
    }
}

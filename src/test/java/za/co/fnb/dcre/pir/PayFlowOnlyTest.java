package za.co.fnb.dcre.pir;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PIR responds on the payments leg ONLY. Same guard as PRR's and PTV's, adapted to
 * what a RESPONDER can get wrong.
 *
 * <p>CIR carries no {@code flow} constant to strip, because it never branched on one:
 * it composes an ACK/NACK from whatever verdicts it finds. What it does carry is a
 * COLLECTIONS-shaped identity, and that is the thing a rename leaves behind silently.
 * Three surfaces, all asserted here:
 *
 * <ul>
 *   <li>the DATABASE. CIR reads {@code dcre_col}; a PIR that kept that URL would
 *       compose payments responses out of collections verdicts and be green all the
 *       way, because the schemas are identical.</li>
 *   <li>the ROUTE tokens. Route is part of arrival identity (A-45) and it is baked into
 *       the response FILENAME, so a leftover {@code onhost-req} names a payments
 *       response after a collections route.</li>
 *   <li>the VERDICT vocabulary. {@code FAIL_ACCOUNT_NOT_FOUND} and every
 *       {@code FAIL_MANDATE_*} are DC-only verdicts PTV cannot emit; a fixture built
 *       on them tests a shape the payments leg never produces.</li>
 * </ul>
 */
class PayFlowOnlyTest {

    @Test
    void noSourceCarriesAFlowDiscriminator() throws Exception {
        assertThat(offenders(Path.of("src/main/java"),
                "FLOW_PAY", "validatedFlow", "\"COL\"", "dcFlow", "flow-dc"))
                .as("PIR serves one family. A flow branch here means collections logic"
                        + " was carried across instead of left behind")
                .isEmpty();
    }

    @Test
    void noSourceNamesACollectionsRouteOrADcOnlyVerdict() throws Exception {
        assertThat(offenders(Path.of("src/main/java"),
                "onhost-req\"", "onhost-req-endo", "FAIL_MANDATE", "FAIL_ACCOUNT_NOT_FOUND",
                "FAIL_EXCEEDS_MANDATE_CAP"))
                .as("payments routes are onhost-req-pay and fint-resp-pay, and the payments"
                        + " verdict vocabulary has no mandate tier and no NOT_FOUND")
                .isEmpty();
    }

    /**
     * The FIXTURES, which are where a responder's collections identity actually hides:
     * the production code is verdict-agnostic, so only the test data says which family
     * this service serves. A fixture asserting a DC-only verdict proves nothing about
     * the payments leg.
     */
    @Test
    void noFixtureAssertsADcOnlyVerdictOrACollectionsRoute() throws Exception {
        assertThat(offenders(Path.of("src/test"),
                "onhost-req\"", "onhost-req-endo", "FAIL_MANDATE", "FAIL_ACCOUNT_NOT_FOUND",
                "FAIL_EXCEEDS_MANDATE_CAP"))
                .as("every seeded verdict must be one PTV can emit, and every route token"
                        + " must be a payments route")
                .isEmpty();
    }

    /** The datasource this service targets is the payments one, and only that one. */
    @Test
    void theConfiguredDatabaseIsDcrePay() throws Exception {
        String yml = Files.readString(Path.of("src/main/resources/application.yml"))
                .replaceAll("(?m)^\\s*#.*$", " ");
        assertThat(yml)
                .as("PIR reads the payments spine and verdicts; the database is the family"
                        + " discriminator now, so reading dcre_col would compose a payments"
                        + " response out of collections rows and never complain")
                .contains("${DCRE_PAY_DB_URL:jdbc:postgresql://localhost:26257/dcre_pay")
                .doesNotContain("dcre_col");
    }

    /** The ledger table and the Batch metadata are per-service, and this is the service. */
    @Test
    void theLedgerAndBatchMetadataArePirOwned() throws Exception {
        String yml = Files.readString(Path.of("src/main/resources/application.yml"));
        assertThat(yml).contains("pir_databasechangelog").contains("PIR_BATCH_");
        String changelog = stripComments(Path.of(
                "src/main/resources/db/changelog/2026/08/001-pir-response.xml"),
                Files.readString(Path.of(
                        "src/main/resources/db/changelog/2026/08/001-pir-response.xml")));
        assertThat(changelog)
                .as("the response ledger is pir_response in dcre_pay, not cir_response")
                .contains("tableName=\"pir_response\"")
                .doesNotContain("cir_response");
        assertThat(changelog)
                .as("A-79/A-81: a bootstrap guard uses CONTINUE; MARK_RAN records the skip"
                        + " permanently and has cost this project two defects")
                .contains("onFail=\"CONTINUE\"")
                .doesNotContain("MARK_RAN");
    }

    /**
     * Scans what an artifact DOES, with comments stripped, for the reason given in
     * PTV's copy of this guard: this repo's javadoc, changelog comments and feature
     * preamble all NAME the collections verdicts and routes that were deliberately
     * left behind, and a scan counting those mentions would push an author to delete
     * the explanation rather than the coupling. Comments never execute; the assertions
     * on the yml, the changelog body and the fixtures are what carry the weight.
     */
    private static List<Path> offenders(Path root, String... tokens) throws Exception {
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().equals("PayFlowOnlyTest.java"))
                    .filter(p -> {
                        try {
                            String source = stripComments(p, Files.readString(p));
                            return Arrays.stream(tokens).anyMatch(source::contains);
                        } catch (Exception e) {
                            throw new IllegalStateException(p.toString(), e);
                        }
                    })
                    .toList();
        }
    }

    /** Comment syntax by extension: Java block/line, XML, and hash-comment resources. */
    private static String stripComments(Path path, String source) {
        String name = path.getFileName().toString();
        if (name.endsWith(".java")) {
            return source.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("(?m)//.*$", " ");
        }
        if (name.endsWith(".xml")) {
            return source.replaceAll("(?s)<!--.*?-->", " ");
        }
        return source.replaceAll("(?m)^\\s*#.*$", " ");
    }
}

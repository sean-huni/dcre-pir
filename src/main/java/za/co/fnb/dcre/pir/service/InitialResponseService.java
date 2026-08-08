package za.co.fnb.dcre.pir.service;

import org.springframework.stereotype.Service;
import za.co.fnb.dcre.pir.data.model.TxHeaderView;
import za.co.fnb.dcre.pir.data.model.VerdictView;
import za.co.fnb.dcre.pir.data.repo.TxHeaderViewRepo;
import za.co.fnb.dcre.pir.data.repo.VerdictViewRepo;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;
import za.co.fnb.dcre.platform.files.StagedWrite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Business tier (configuration.md point 21): composes the initial ACK/NACK
 * for one arrival and stages it to the OnHost response directory.
 * SYNTHETIC-CONTRACT format pending the response-copybook recovery (Q-9).
 * ACK means accepted-by-DCRE (Fugu F11). StagedWrite = restart no-op (R-05).
 */
@Service
public class InitialResponseService {

    public record Result(Path responseFile, boolean written) { }

    // route tokens are hyphen-only lowercase constants (onhost-req-pay, fint-resp-pay, fint-resp)
    private static final Pattern ROUTE_TOKEN = Pattern.compile("[a-z0-9-]+");

    // pir_response.reason column width (spec 1.2 / 11.2): free-text reasons are clipped to fit.
    private static final int REASON_MAX = 64;

    // pir_response.client column width (003-pir-response.xml). Its source, tx_header.initg_pty, is
    // VARCHAR(35): an over-length client identity fails closed (identity is never clipped: see stage()).
    private static final int CLIENT_MAX = 16;

    /** The ACK/NACK outcome plus the acceptance ratio and NACK reason captured into pir_response. */
    private record Decision(String outcome, String reason, Integer acceptedCount, Integer totalCount) { }

    private final TxHeaderViewRepo headers;
    private final VerdictViewRepo verdicts;
    private final ExchangeLayout layout;
    private final ResponseLedgerWriter ledger;

    public InitialResponseService(final TxHeaderViewRepo headers, final VerdictViewRepo verdicts,
                                  final ExchangeLayout layout, final ResponseLedgerWriter ledger) {
        this.headers = headers;
        this.verdicts = verdicts;
        this.layout = layout;
        this.ledger = ledger;
    }

    public Result respond(final UUID arrivalId, final String route, final String fatalReason,
                          final String clientToken, final String msgId,
                          final String outcomeHint) throws IOException {
        if (route == null || !ROUTE_TOKEN.matcher(route).matches()) {
            // A-45: route is part of the arrival identity; a fallback token would
            // re-create the (client, msgId) collision class, so fail the job instead.
            // The whitelist also keeps the token from escaping onhost-resp as a path.
            throw new IllegalArgumentException(
                    "arrival route missing or invalid: required for response identity (A-45)");
        }
        Optional<TxHeaderView> maybeHeader = headers.findByArrivalId(arrivalId);
        if (maybeHeader.isEmpty()) {
            // A-42: PRR fataled before persisting the header; identity comes from AGT job
            // params. A 1.x AGT sends none: fall back to UNKNOWN + arrivalId so the file
            // name stays per-arrival unique and restart-stable (R-05 no-op semantics).
            String client = hasText(clientToken) ? clientToken : "UNKNOWN";
            String responseMsgId = hasText(msgId) ? msgId : arrivalId.toString();
            String reason = fatalReason != null ? fatalReason : "NO_HEADER";
            return stage(arrivalId, client, responseMsgId, route,
                    List.of("NACK|" + client + "|" + responseMsgId + "|0/0|" + reason),
                    new Decision("NACK", reason, 0, 0));
        }
        TxHeaderView header = maybeHeader.get();
        String client = header.getInitgPty().strip();
        String headerMsgId = header.getMsgId().strip();
        int total = header.getTxCount();

        List<VerdictView> rejects = verdicts.findByArrivalIdAndOutcomeNotOrderBySequence(arrivalId, "PASS");
        long verdictCount = verdicts.countByArrivalId(arrivalId);

        List<String> lines = new ArrayList<>();
        Decision decision;
        if ("BUSINESS_FILE_REJECTED".equals(outcomeHint)) {
            // R-41 ALL_OR_NOTHING: whole file refused by policy, itemized per non-PASS verdict
            lines.add("NACK|" + client + "|" + headerMsgId + "|0/" + total + "|FILE_REJECTED_BY_POLICY");
            for (VerdictView reject : rejects) {
                lines.add("REJ|" + reject.getSequence() + "|" + reject.getOutcome());
            }
            decision = new Decision("NACK", "FILE_REJECTED_BY_POLICY", 0, total);
        } else if (fatalReason != null || verdictCount == 0) {
            String reason = fatalReason != null ? fatalReason : "NO_VERDICTS";
            lines.add("NACK|" + client + "|" + headerMsgId + "|0/" + total + "|" + reason);
            decision = new Decision("NACK", reason, 0, total);
        } else {
            int accepted = total - rejects.size();
            lines.add("ACK|" + client + "|" + headerMsgId + "|" + accepted + "/" + total + "|ACCEPTED_BY_DCRE");
            for (VerdictView reject : rejects) {
                lines.add("REJ|" + reject.getSequence() + "|" + reject.getOutcome());
            }
            // A clean/partial ACK carries no NACK reason; the ratio lives in accepted/total.
            decision = new Decision("ACK", null, accepted, total);
        }
        return stage(arrivalId, client, headerMsgId, route, lines, decision);
    }

    private static boolean hasText(final String value) {
        return value != null && !value.isBlank();
    }

    private Result stage(final UUID arrivalId, final String client, final String msgId, final String route,
                         final List<String> lines, final Decision decision) throws IOException {
        // M2 fail-closed: pir_response.client is VARCHAR(16) but its source tx_header.initg_pty is
        // VARCHAR(35). An over-length client identity is an upstream contract violation; reject it
        // with a readable business message BEFORE the write-ahead insert (consistent with the
        // fail-closed-on-unconfigured-client posture) so it never surfaces as a raw CRDB "value too
        // long for type varchar(16)" that kills the job. Identity is NEVER clipped (unlike reason);
        // this only guards the column width and does NOT resolve A-43 (canonical client-token source).
        if (client.length() > CLIENT_MAX) {
            throw new IllegalStateException(
                    "pir_response.client identity exceeds %d chars: %s".formatted(CLIENT_MAX, client));
        }
        // SCRUM-42: per-client dir <root>/<base>/onhost-resp/out; fail-closed for an
        // unconfigured client (e.g. the A-42 "UNKNOWN" fallback has no dir, so the job fails).
        // A-45: (client, msgId) repeats across routes as distinct arrivals; the route token
        // keeps the R-05 idempotency key (the filename) aligned with the full arrival identity.
        // Resolve BEFORE any capture: a fail-closed config error writes neither a row nor a file.
        String fileName = "%s_%s_%s_RESP.txt".formatted(client, msgId, route);
        Path target = layout.resolve(client, ExchangeChannel.ONHOST_RESP, ExchangeSub.OUT).resolve(fileName);
        // SCRUM-58 write-ahead: the filename row commits (REQUIRES_NEW) BEFORE the file exists, so a
        // kill between here and the write leaves a written_at IS NULL stuck signal; the table is the
        // system of record. The insert is idempotent on arrival_id, StagedWrite is a restart no-op
        // (R-05), and the stamp is guarded, so a resume no-ops the row and stamps exactly once.
        ledger.stage(arrivalId, client, msgId, route, decision.outcome(), fileName,
                clip(decision.reason()), decision.acceptedCount(), decision.totalCount());
        boolean written = StagedWrite.write(target, lines);
        ledger.stampWritten(arrivalId);
        return new Result(target, written);
    }

    /**
     * pir_response.reason is VARCHAR(64) descriptive free text; a free-text fatalReason job param
     * can exceed that. Clip for the row (the full reason stays in the response file line) so an
     * over-length reason never turns a describable NACK into a hard write-ahead job failure.
     * Identity columns (client, msg_id) are deliberately NOT clipped: an over-length identity is a
     * genuine upstream contract violation and must fail closed, aligned with the file_name identity.
     */
    private static String clip(final String reason) {
        return reason != null && reason.length() > REASON_MAX ? reason.substring(0, REASON_MAX) : reason;
    }
}

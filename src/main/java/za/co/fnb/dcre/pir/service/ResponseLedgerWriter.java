package za.co.fnb.dcre.pir.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.pir.data.repo.PirResponseRepo;

import java.util.UUID;

/**
 * SCRUM-58 write-ahead capture: durably records the response filename in
 * pir_response BEFORE the file is staged, and stamps it AFTER, so the DB is the
 * system of record and a NULL {@code written_at} is a staged-not-written signal.
 *
 * <p>Each op runs in its OWN REQUIRES_NEW transaction so the row commits
 * independently of the surrounding batch step (that is the write-ahead
 * guarantee) and a CRDB 40001 abort retries in a fresh transaction (an aborted
 * transaction poisons every further statement, 25P02). SRP split from
 * InitialResponseService so that one composes the response and this one owns
 * the ledger transaction plumbing.
 */
@Service
public class ResponseLedgerWriter {

    private final PirResponseRepo repo;
    private final TransactionTemplate requiresNew;

    public ResponseLedgerWriter(final PirResponseRepo repo, final PlatformTransactionManager txManager) {
        this.repo = repo;
        this.requiresNew = new TransactionTemplate(txManager);
        this.requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /** Commit the filename row write-ahead of the filesystem effect (idempotent on arrival_id). */
    public void stage(final UUID arrivalId, final String client, final String msgId, final String routeId,
                      final String outcome, final String fileName, final String reason,
                      final Integer acceptedCount, final Integer totalCount) {
        CrdbRetry.run("stage pir_response arrival=%s".formatted(arrivalId),
                () -> requiresNew.execute(status -> {
                    repo.insertStaged(arrivalId, client, msgId, routeId, outcome,
                            fileName, reason, acceptedCount, totalCount);
                    return null;
                }));
    }

    /** Stamp {@code written_at} once the file is on disk (guarded no-op if already stamped). */
    public void stampWritten(final UUID arrivalId) {
        CrdbRetry.run("stamp pir_response arrival=%s".formatted(arrivalId),
                () -> requiresNew.execute(status -> {
                    repo.stampWritten(arrivalId);
                    return null;
                }));
    }
}

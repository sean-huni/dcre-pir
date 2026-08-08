package za.co.fnb.dcre.pir.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.pir.data.model.PirResponseEntity;

import java.util.Optional;
import java.util.UUID;

/**
 * SCRUM-58 pir_response ledger access. Writes are native so idempotency lives
 * in the database, not in application check-then-act (persistence.md):
 * {@code insertStaged} conflicts on the full business identity {@code arrival_id}
 * (route is baked into that identity per A-45), so a restart, a step-level retry
 * or a job relaunch is a silent ON CONFLICT no-op rather than a duplicate row.
 * CRDB UPSERT resolves on the PK only, hence explicit ON CONFLICT. {@code id}
 * and {@code created_at} are DB-assigned ({@code gen_random_uuid()}/{@code now()}).
 */
public interface PirResponseRepo extends Repository<PirResponseEntity, UUID> {

    /**
     * Write-ahead capture, committed BEFORE the response file is staged. Idempotent
     * on {@code arrival_id}: a re-run keeps the first row unchanged (DO NOTHING).
     */
    @Modifying
    @Query("""
            INSERT INTO pir_response
                (arrival_id, client, msg_id, route_id, outcome, file_name, reason, accepted_count, total_count)
            VALUES
                (:arrivalId, :client, :msgId, :routeId, :outcome, :fileName, :reason, :acceptedCount, :totalCount)
            ON CONFLICT (arrival_id) DO NOTHING""")
    void insertStaged(@Param("arrivalId") UUID arrivalId, @Param("client") String client,
                      @Param("msgId") String msgId, @Param("routeId") String routeId,
                      @Param("outcome") String outcome, @Param("fileName") String fileName,
                      @Param("reason") String reason, @Param("acceptedCount") Integer acceptedCount,
                      @Param("totalCount") Integer totalCount);

    /**
     * Guarded stamp after the file is on disk. The {@code written_at IS NULL} guard
     * makes it a no-op once stamped, so a resume never overwrites a prior timestamp.
     */
    @Modifying
    @Query("UPDATE pir_response SET written_at = now() WHERE arrival_id = :arrivalId AND written_at IS NULL")
    void stampWritten(@Param("arrivalId") UUID arrivalId);

    @Query("SELECT * FROM pir_response WHERE arrival_id = :arrivalId")
    Optional<PirResponseEntity> findByArrivalId(@Param("arrivalId") UUID arrivalId);
}

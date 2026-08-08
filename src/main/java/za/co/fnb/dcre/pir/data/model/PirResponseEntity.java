package za.co.fnb.dcre.pir.data.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * SCRUM-58 write-ahead filename ledger (PIR's first domain table): one row per
 * arrival, committed BEFORE the response file is staged, and the system of
 * record for every initial ACK/NACK. {@code arrival_id} and {@code file_name}
 * are unique; {@code route_id} is part of identity (A-45 cross-route twins).
 * {@code written_at} NULL after commit = staged-not-written stuck signal.
 *
 * <p>Deliberately does NOT extend {@code BaseEntity}: this is an append-mostly
 * capture ledger written via native {@code INSERT ... ON CONFLICT DO NOTHING}
 * plus a guarded {@code written_at} stamp, never {@code save()}, so it needs no
 * optimistic-lock version or {@code updated_at} audit column. {@code id}
 * ({@code gen_random_uuid()}) and {@code created_at} ({@code now()}) are
 * DB-assigned on the insert (same shape as rpt_run). Read-only aggregate.
 */
@Table("pir_response")
public class PirResponseEntity {

    @Id
    private UUID id;
    private UUID arrivalId;
    private String client;
    private String msgId;
    private String routeId;
    private String outcome;
    private String fileName;
    private String reason;
    private Integer acceptedCount;
    private Integer totalCount;
    private Instant writtenAt;
    private Instant createdAt;

    public UUID getId() { return id; }
    public UUID getArrivalId() { return arrivalId; }
    public String getClient() { return client; }
    public String getMsgId() { return msgId; }
    public String getRouteId() { return routeId; }
    public String getOutcome() { return outcome; }
    public String getFileName() { return fileName; }
    public String getReason() { return reason; }
    public Integer getAcceptedCount() { return acceptedCount; }
    public Integer getTotalCount() { return totalCount; }
    public Instant getWrittenAt() { return writtenAt; }
    public Instant getCreatedAt() { return createdAt; }
}

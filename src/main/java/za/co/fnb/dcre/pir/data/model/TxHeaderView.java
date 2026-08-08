package za.co.fnb.dcre.pir.data.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

/** Read model over PRR's tx_header (grants-based, R-04/R-06). */
@Table("tx_header")
public class TxHeaderView {

    @Id
    private UUID id;
    private UUID arrivalId;
    private String msgId;
    private String initgPty;
    private Integer txCount;

    public UUID getArrivalId() { return arrivalId; }
    public String getMsgId() { return msgId; }
    public String getInitgPty() { return initgPty; }
    public Integer getTxCount() { return txCount; }
}

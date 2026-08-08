package za.co.fnb.dcre.pir.data.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

/** Read model over PTV's validation_log. */
@Table("validation_log")
public class VerdictView {

    @Id
    private UUID id;
    private UUID arrivalId;
    private Integer sequence;
    private String outcome;

    public Integer getSequence() { return sequence; }
    public String getOutcome() { return outcome; }
}

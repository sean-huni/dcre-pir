package za.co.fnb.dcre.pir.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.pir.data.model.VerdictView;

import java.util.List;
import java.util.UUID;

public interface VerdictViewRepo extends CrudRepository<VerdictView, UUID> {

    List<VerdictView> findByArrivalIdAndOutcomeNotOrderBySequence(UUID arrivalId, String outcome);

    long countByArrivalId(UUID arrivalId);
}

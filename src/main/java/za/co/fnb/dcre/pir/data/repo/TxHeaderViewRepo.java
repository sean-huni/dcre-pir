package za.co.fnb.dcre.pir.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.pir.data.model.TxHeaderView;

import java.util.Optional;
import java.util.UUID;

public interface TxHeaderViewRepo extends CrudRepository<TxHeaderView, UUID> {

    Optional<TxHeaderView> findByArrivalId(UUID arrivalId);
}

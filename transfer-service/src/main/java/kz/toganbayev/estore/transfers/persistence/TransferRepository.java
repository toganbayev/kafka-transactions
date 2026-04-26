package kz.toganbayev.estore.transfers.persistence;

import org.h2.value.Transfer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransferRepository extends JpaRepository<TransferEntity, String> {
}

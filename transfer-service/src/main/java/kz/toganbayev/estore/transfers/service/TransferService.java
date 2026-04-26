package kz.toganbayev.estore.transfers.service;

import kz.toganbayev.estore.transfers.model.TransferRestModel;

public interface TransferService {
    public boolean transfer(TransferRestModel productPaymentRestModel);
}

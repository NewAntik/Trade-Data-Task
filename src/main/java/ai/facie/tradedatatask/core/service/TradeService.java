package ai.facie.tradedatatask.core.service;

import ai.facie.tradedatatask.core.model.Trade;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TradeService {

	CompletableFuture<List<Trade>> enrichTradesAsync(InputStream stream);

}

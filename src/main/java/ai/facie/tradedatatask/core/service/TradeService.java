package ai.facie.tradedatatask.core.service;

import reactor.core.publisher.Flux;

import java.io.InputStream;

public interface TradeService {

	Flux<String> enrichTradesStream(InputStream stream);

}

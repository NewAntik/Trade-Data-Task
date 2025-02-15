package ai.facie.tradedatatask.controller;

import ai.facie.tradedatatask.core.service.TradeService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.io.IOException;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/trades")
public class TradeController {
	public static final String TABLE_HEADER = "date,productName,currency,price\n";

	private final TradeService tradeService;

	/**
	 * Processes an uploaded trade file reactively and returns a streamed response.
	 *
	 * <p>Streams trade processing results in real-time without holding the entire file in memory.</p>
	 *
	 * @param file The uploaded trade file in CSV format.
	 * @return A {@link Flux} containing enriched trade records as a streamed response.
	 */
	@PostMapping(consumes = "multipart/form-data", produces = MediaType.TEXT_PLAIN_VALUE)
	public Flux<String> enrichTrades(@RequestParam("file") MultipartFile file) throws IOException {
		log.info("Processing file reactively: {}", file.getOriginalFilename());
		if (file.isEmpty()) {
			return Flux.just("Upload failed: The file is empty.");
		}
		final Flux<String> treads = tradeService.enrichTradesStream(file.getInputStream());

		return treads.map(trade -> trade + System.lineSeparator())
			.startWith(TABLE_HEADER);
	}
}
package ai.facie.tradedatatask.controller;

import ai.facie.tradedatatask.controller.util.ResponseFormatter;
import ai.facie.tradedatatask.core.model.Trade;
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/trades")
public class TradeController {

	private final TradeService tradeService;

	/**
	 * Processes an uploaded trade file, enriches the data, and returns it as a formatted string.
	 *
	 * <p>This method reads the uploaded file, validates trade records, enriches them with product names,
	 * and returns the results as a structured string .</p>
	 *
	 * @param file The uploaded trade file in CSV format.
	 * @return A {@link CompletableFuture} containing a response with a formatted string of trade data.
	 * @throws IOException If an error occurs while reading the file.
	 */
	@PostMapping(consumes = "multipart/form-data", produces = MediaType.TEXT_PLAIN_VALUE)
	public CompletableFuture<ResponseEntity<String>> enrichTrades(
		@RequestParam("file") final MultipartFile file
	) throws IOException {
		log.info("enrichTrade was called with file name: {}", file.getOriginalFilename());
		final CompletableFuture<List<Trade>> tradeFuture = tradeService.enrichTradesAsync(file.getInputStream());

		return tradeFuture.thenApply(trades -> {
			final String formattedResponse = ResponseFormatter.format(trades); // Convert trade data to a formatted string

			return ResponseEntity.ok()
				.contentType(MediaType.TEXT_PLAIN)
				.body(formattedResponse);
		});
	}
}
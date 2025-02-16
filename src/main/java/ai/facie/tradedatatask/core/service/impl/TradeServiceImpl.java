package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import ai.facie.tradedatatask.core.service.TradeService;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

@Slf4j
@Service
@AllArgsConstructor
public class TradeServiceImpl implements TradeService {
	private static final int START_LINE = 1;
	private static final String DATE_TIME_FORMAT = "yyyyMMdd";
	private static final String SKIPPING_MESSAGE = "Skipping invalid trade record: {}";
	private static final Pattern CSV_SPLIT_PATTERN = Pattern.compile(",");
	private static final int BATCH_SIZE = 1000;
	public static final String TABLE_HEADER = "date,productName,currency,price\n";

	private final ProductService productService;

	/**
	 * Processes trade records reactively using multi-threading.
	 *
	 * <p>Reads the file line-by-line, validates trade data, enriches it in parallel,
	 * and streams the results.</p>
	 *
	 * @param stream The input stream containing trade data.
	 *
	 * @return A {@link Flux} that emits enriched trade records as a stream.
	 */
	@Override
	@SneakyThrows
	public Flux<String> enrichTradesStream(final InputStream stream) {
		return Flux.using(
			() -> new BufferedReader(new InputStreamReader(stream)),
			reader -> Flux.fromStream(reader.lines().skip(START_LINE))
				.parallel()
				.runOn(Schedulers.boundedElastic())
				.map(this::parseTrade)
				.filter(Objects::nonNull)
				.sequential()
				.buffer(BATCH_SIZE)
				.flatMap(this::fetchProductNamesInBatch)
				.startWith(TABLE_HEADER),
			reader -> {
				Schedulers.boundedElastic().schedule(() -> {
					try {
						reader.close();
					} catch (final IOException e) {
						log.error("Error closing BufferedReader", e);
					}
				});
			}
		);
	}

	/**
	 * Parses a trade CSV line and extracts the trade details.
	 *
	 * @param line A single line from the trade CSV.
	 * @return A parsed TradeRecord or null if invalid.
	 */
	private TradeRecord parseTrade(final String line) {
		final String[] parts = CSV_SPLIT_PATTERN.split(line);

		if (parts.length == 4 && isValidDate(parts[0])) {
			try {
				return new TradeRecord(parts[0], Long.parseLong(parts[1]), parts[2], parts[3]);
			} catch (NumberFormatException e) {
				log.warn("Skipping invalid product ID: {}", parts[1]);
			}
		} else {
			log.warn(SKIPPING_MESSAGE, line);
		}
		return null;
	}

	/**
	 * Fetches product names in batch from Redis, reducing the number of calls.
	 *
	 * <p>Formats each trade record with a newline separator in parallel before returning.</p>
	 *
	 * @param batch List of trade records.
	 * @return Flux<String> containing enriched trade records with newlines.
	 */
	private Flux<String> fetchProductNamesInBatch(final List<TradeRecord> batch) {
		final List<String> productIds = batch.stream()
			.map(trade -> String.valueOf(trade.productId()))
			.toList();

		final List<String> productNames = productService.getProductNamesInBatch(productIds);

		return Flux.fromIterable(batch)
			.parallel()
			.runOn(Schedulers.parallel())
			.map(trade -> {
				String productName = productNames.get(batch.indexOf(trade));
				return trade.date() + "," + productName + "," + trade.currency() + "," + trade.price() + System.lineSeparator();
			})
			.sequential();
	}

	/**
	 * Validates whether the given date string is in the expected format ({@code yyyyMMdd}).
	 *
	 * @param dateStr The date string to validate.
	 *
	 * @return {@code true} if the date format is valid, otherwise {@code false}.
	 */
	private boolean isValidDate(final String dateStr) {
		try {
			LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(DATE_TIME_FORMAT));

			return true;
		} catch (final DateTimeParseException e) {
			return false;
		}
	}

	/**
	 * Represents a parsed trade record.
	 */
	private record TradeRecord(String date, Long productId, String currency, String price) {}
}
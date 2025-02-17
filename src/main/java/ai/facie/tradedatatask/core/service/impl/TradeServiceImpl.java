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
	private static final int BATCH_SIZE = 1000;
	private static final String DATE_TIME_FORMAT = "yyyyMMdd";
	private static final Pattern CSV_SPLIT_PATTERN = Pattern.compile(",");
	private static final String SKIPPING_MESSAGE = "Skipping invalid trade record: {}";
	public static final String TABLE_HEADER = "date,productName,currency,price\n";

	private final ProductService productService;

	/**
	 * Enriches trade data from an input stream.
	 *
	 * @param stream Input stream containing trade data.
	 * @return A Flux stream of processed trade records.
	 */
	@Override
	@SneakyThrows
	public Flux<String> enrichTradesStream(final InputStream stream) {
		return Flux.using(
			() -> createBufferedReader(stream),
			this::processTradeStream,
			this::closeBufferedReader
		);
	}

	/**
	 * Creates a buffered reader for reading the input stream.
	 *
	 * @param stream Input stream.
	 * @return BufferedReader instance.
	 */
	private BufferedReader createBufferedReader(final InputStream stream) {
		return new BufferedReader(new InputStreamReader(stream));
	}

	/**
	 * Processes the trade data stream, parses and enriches trade records.
	 *
	 * @param reader BufferedReader for reading the trade data.
	 * @return A Flux stream of formatted trade records.
	 */
	private Flux<String> processTradeStream(final BufferedReader reader) {
		return Flux.fromStream(reader.lines().skip(START_LINE))
			.parallel()
			.runOn(Schedulers.boundedElastic())
			.map(this::parseTrade)
			.filter(Objects::nonNull)
			.sequential()
			.buffer(BATCH_SIZE)
			.flatMap(this::fetchProductNamesInBatch)
			.startWith(TABLE_HEADER);
	}

	/**
	 * Closes the BufferedReader instance safely.
	 *
	 * @param reader BufferedReader instance to close.
	 */
	private void closeBufferedReader(final BufferedReader reader) {
		Schedulers.boundedElastic().schedule(() -> {
			try {
				reader.close();
			} catch (final IOException e) {
				log.error("Error closing BufferedReader", e);
			}
		});
	}

	/**
	 * Parses a single trade record line into a TradeRecord object.
	 *
	 * @param line A line from the input data.
	 * @return A parsed TradeRecord object or null if invalid.
	 */
	private TradeRecord parseTrade(final String line) {
		final String[] parts = CSV_SPLIT_PATTERN.split(line);
		if (isValidTradeRecord(parts)) {
			return createTradeRecord(parts);
		}
		log.warn(SKIPPING_MESSAGE, line);

		return null;
	}

	/**
	 * Validates a trade record based on the expected data format.
	 *
	 * @param parts Array of trade record fields.
	 * @return true if the trade record is valid, otherwise false.
	 */
	private boolean isValidTradeRecord(final String[] parts) {
		return parts.length == 4 && isValidDate(parts[0]);
	}

	/**
	 * Creates a TradeRecord object from parsed fields.
	 *
	 * @param parts Array of trade record fields.
	 * @return A TradeRecord object or null if parsing fails.
	 */
	private TradeRecord createTradeRecord(final String[] parts) {
		try {
			return new TradeRecord(parts[0], Long.parseLong(parts[1]), parts[2], parts[3]);
		} catch (NumberFormatException e) {
			log.warn("Skipping invalid product ID: {}", parts[1]);

			return null;
		}
	}

	/**
	 * Fetches product names for a batch of trade records.
	 *
	 * @param batch List of TradeRecord objects.
	 * @return A Flux stream of formatted trade records with enriched product names.
	 */
	private Flux<String> fetchProductNamesInBatch(final List<TradeRecord> batch) {
		final List<String> productIds = batch.stream().map(trade -> String.valueOf(trade.productId())).toList();
		final List<String> productNames = productService.getProductNamesInBatch(productIds);

		return mapTradesToTable(batch, productNames);
	}

	/**
	 * Maps trade records to a formatted output including product names.
	 *
	 * @param batch List of TradeRecord objects.
	 * @param productNames List of product names corresponding to the batch.
	 * @return A Flux stream of formatted trade records.
	 */
	private Flux<String> mapTradesToTable(final List<TradeRecord> batch, final List<String> productNames) {
		return Flux.fromIterable(batch)
			.parallel()
			.runOn(Schedulers.parallel())
			.map(trade -> formatTradeRecord(trade, productNames.get(batch.indexOf(trade))))
			.sequential();
	}

	/**
	 * Formats a trade record into a structured output line.
	 *
	 * @param trade TradeRecord object.
	 * @param productName Enriched product name.
	 * @return A formatted trade record string.
	 */
	private String formatTradeRecord(final TradeRecord trade, final String productName) {
		return trade.date() + "," + productName + "," + trade.currency() + "," + trade.price() + System.lineSeparator();
	}

	/**
	 * Validates if a given date string matches the expected format.
	 *
	 * @param dateStr Date string to validate.
	 * @return true if the date is valid, otherwise false.
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
	 * Represents a trade record with essential fields.
	 */
	record TradeRecord(String date, Long productId, String currency, String price) {}
}
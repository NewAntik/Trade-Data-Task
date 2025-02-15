package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.model.Trade;
import ai.facie.tradedatatask.core.service.ProductService;
import ai.facie.tradedatatask.core.service.TradeService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
@AllArgsConstructor
public class TradeServiceImpl implements TradeService {
	private static final int POOL_AMOUNT = 10;
	private static final String DATA_TIME_FORMAT = "yyyyMMdd";
	private static final int START_LINE = 1;
	private static final String SKIPPING_MESSAGE = "Skipping invalid trade record: {}";

	private final ProductService productService;

	private final ExecutorService executorService = Executors.newFixedThreadPool(POOL_AMOUNT);

	/**
	 * Processes trade data asynchronously.
	 *
	 * <p>Reads trade data from an {@link InputStream}, validates each record, enriches it with
	 * product names from Redis, and returns a list of valid {@link Trade} objects.</p>
	 *
	 * <p>This method executes asynchronously using a fixed thread pool of size {@code POOL_AMOUNT}.</p>
	 *
	 * @param stream The input stream containing trade data in CSV format.
	 * @return A {@link CompletableFuture} containing a list of enriched {@link Trade} objects.
	 */
	@Override
	public CompletableFuture<List<Trade>> enrichTradesAsync(final InputStream stream) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return enrichTrades(stream);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}, executorService);
	}

	/**
	 * Reads and processes trade records from an input stream.
	 *
	 * <p>Parses each CSV line, validates trade data, enriches it with product names, and collects
	 * valid records into a list.</p>
	 *
	 * @param stream The input stream containing trade data.
	 * @return A list of valid {@link Trade} objects.
	 * @throws IOException If an error occurs while reading the input stream.
	 */
	private List<Trade> enrichTrades(final InputStream stream) throws IOException {
		final BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

		return reader.lines()
			.skip(START_LINE)
			.map(this::enrichTrade)
			.filter(Objects::nonNull)
			.toList();
	}

	/**
	 * Converts a CSV line into a {@link Trade} object, handling validation and enrichment.
	 *
	 * <p>This method extracts trade details from a CSV row, validates the trade date format,
	 * retrieves the corresponding product name from Redis, and constructs a {@link Trade} object.</p>
	 *
	 * @param line A single line from the CSV file.
	 * @return A valid {@link Trade} object if the line is correctly formatted, otherwise {@code null}.
	 */
	private Trade enrichTrade(final String line) {
		final String[] parts = line.split(",");

		if (parts.length == 4 && isValidDate(parts[0])) {
			final String productName = productService.getProductName(Long.parseLong(parts[1]));
			return new Trade(parts[0], productName, parts[2], new BigDecimal(parts[3]));
		} else {
			log.warn(SKIPPING_MESSAGE, line);
			return null;
		}
	}

	/**
	 * Validates whether the given date string is in the expected format ({@code yyyyMMdd}).
	 *
	 * <p>This method attempts to parse the date and returns {@code true} if successful,
	 * otherwise returns {@code false}.</p>
	 *
	 * @param dateStr The date string to validate.
	 * @return {@code true} if the date format is valid, otherwise {@code false}.
	 */
	private boolean isValidDate(final String dateStr) {
		try {
			LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(DATA_TIME_FORMAT));
			return true;
		} catch (final DateTimeParseException e) {
			return false;
		}
	}
}

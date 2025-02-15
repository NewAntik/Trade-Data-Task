package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import ai.facie.tradedatatask.core.service.TradeService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
	public Flux<String> enrichTradesStream(final InputStream stream) {
		return Flux.<String>create(sink -> enrichTrades(sink, stream))
			.parallel()
			.runOn(Schedulers.boundedElastic())
			.sequential();
	}

	/**
	 * Streams trade data reactively by processing lines from an input stream in parallel.
	 *
	 * <p>Reads trade records line by line, validates, enriches, and emits each valid trade.
	 * Skips invalid records. Marks stream as complete or emits error in case of failure.</p>
	 *
	 * @param sink The {@link FluxSink} used to emit trade records reactively.
	 * @param stream The input stream containing trade data.
	 */
	private void enrichTrades(final FluxSink<String> sink, final InputStream stream) {
		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
			reader.lines()
				.skip(START_LINE)
				.parallel()
				.map(this::enrichTrade)
				.filter(Objects::nonNull)
				.forEach(sink::next);

			sink.complete();
		} catch (Exception e) {
			sink.error(new RuntimeException("Error processing trade file", e));
		}
	}

	/**
	 * Converts a CSV line into a trade string, handling validation and enrichment.
	 *
	 * <p>This method is now thread-safe and optimized for parallel execution.</p>
	 *
	 * @param line A single line from the file.
	 *
	 * @return A formatted string representation of the trade if valid, otherwise null.
	 */
	private String enrichTrade(final String line) {
		final String[] parts = CSV_SPLIT_PATTERN.split(line);

		if (parts.length == 4 && isValidDate(parts[0])) {
			final String productName = productService.getProductName(Long.parseLong(parts[1]));
			return parts[0] + "," + productName + "," + parts[2] + "," + parts[3];
		} else {
			log.warn(SKIPPING_MESSAGE, line);
			return null;
		}
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
}
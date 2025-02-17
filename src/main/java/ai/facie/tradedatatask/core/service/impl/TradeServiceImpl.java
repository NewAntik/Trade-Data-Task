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

	@Override
	@SneakyThrows
	public Flux<String> enrichTradesStream(final InputStream stream) {
		return Flux.using(
			() -> createBufferedReader(stream),
			this::processTradeStream,
			this::closeBufferedReader
		);
	}

	private BufferedReader createBufferedReader(final InputStream stream) {
		return new BufferedReader(new InputStreamReader(stream));
	}

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

	private void closeBufferedReader(final BufferedReader reader) {
		Schedulers.boundedElastic().schedule(() -> {
			try {
				reader.close();
			} catch (final IOException e) {
				log.error("Error closing BufferedReader", e);
			}
		});
	}

	private TradeRecord parseTrade(final String line) {
		final String[] parts = CSV_SPLIT_PATTERN.split(line);
		if (isValidTradeRecord(parts)) {
			return createTradeRecord(parts);
		}
		log.warn(SKIPPING_MESSAGE, line);

		return null;
	}

	private boolean isValidTradeRecord(final String[] parts) {
		return parts.length == 4 && isValidDate(parts[0]);
	}

	private TradeRecord createTradeRecord(final String[] parts) {
		try {
			return new TradeRecord(parts[0], Long.parseLong(parts[1]), parts[2], parts[3]);
		} catch (NumberFormatException e) {
			log.warn("Skipping invalid product ID: {}", parts[1]);

			return null;
		}
	}

	private Flux<String> fetchProductNamesInBatch(final List<TradeRecord> batch) {
		final List<String> productIds = batch.stream().map(trade -> String.valueOf(trade.productId())).toList();
		final List<String> productNames = productService.getProductNamesInBatch(productIds);

		return mapTradesToCsv(batch, productNames);
	}

	private Flux<String> mapTradesToCsv(final List<TradeRecord> batch, final List<String> productNames) {
		return Flux.fromIterable(batch)
			.parallel()
			.runOn(Schedulers.parallel())
			.map(trade -> formatTradeRecord(trade, productNames.get(batch.indexOf(trade))))
			.sequential();
	}

	private String formatTradeRecord(final TradeRecord trade, final String productName) {
		return trade.date() + "," + productName + "," + trade.currency() + "," + trade.price() + System.lineSeparator();
	}

	private boolean isValidDate(final String dateStr) {
		try {
			LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(DATE_TIME_FORMAT));
			return true;
		} catch (final DateTimeParseException e) {
			return false;
		}
	}

	record TradeRecord(String date, Long productId, String currency, String price) {}
}
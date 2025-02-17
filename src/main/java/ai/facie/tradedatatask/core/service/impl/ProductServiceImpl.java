package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@AllArgsConstructor
public class ProductServiceImpl implements ProductService {
	private static final String MISSING_PRODUCT_NAME = "Missing Product Name";
	private static final int START_LINE = 1;
	private static final int BATCH_SIZE = 1000;

	private final RedisTemplate<String, String> redisTemplate;

	/**
	 * Loads product data from an input stream and processes it asynchronously.
	 *
	 * @param stream The input stream containing product data.
	 */
	@Override
	public void loadProductsFromStream(final InputStream stream) {
		log.info("Starting to load products from stream.");

		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
			processProductStream(reader);
		} catch (final IOException e) {
			log.error("Error reading from input stream", e);
		}
	}

	/**
	 * Retrieves product names from Redis for the given list of product IDs.
	 * If a product ID is not found, it is replaced with a placeholder.
	 *
	 * @param productIds List of product IDs to fetch names for.
	 * @return List of product names corresponding to the given IDs.
	 */
	@Override
	public List<String> getProductNamesInBatch(final List<String> productIds) {
		final List<String> productNames = fetchProductNamesFromRedis(productIds);

		return replaceMissingProductNames(productIds, productNames);
	}

	/**
	 * Processes the input streamline by line, parses product data, and stores it in Redis in batches.
	 *
	 * @param reader BufferedReader reading the input stream.
	 */
	private void processProductStream(final BufferedReader reader) {
		Flux.fromStream(reader.lines().skip(START_LINE))
			.map(this::parseProduct)
			.filter(Objects::nonNull)
			.buffer(BATCH_SIZE)
			.doOnNext(this::batchInsertToRedis)
			.blockLast();
	}

	/**
	 * Parses a single line from the input stream into a key-value product entry.
	 *
	 * @param line A line containing product.
	 * @return A map entry with product ID as key and product name as value, or null if invalid.
	 */
	private Map.Entry<String, String> parseProduct(final String line) {
		final String[] parts = line.split(",");
		if (parts.length == 2) {
			return Map.entry(parts[0], parts[1]);
		} else {
			log.warn("Skipping invalid product record: {}", line);
			return null;
		}
	}

	/**
	 * Stores a batch of product entries into Redis.
	 *
	 * @param batch List of product entries to be inserted.
	 */
	private void batchInsertToRedis(final List<Map.Entry<String, String>> batch) {
		final Map<String, String> productMap = batch.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		redisTemplate.opsForValue().multiSet(productMap);
	}

	/**
	 * Fetches product names from Redis based on a list of product IDs.
	 *
	 * @param productIds List of product IDs.
	 * @return List of product names retrieved from Redis.
	 */
	private List<String> fetchProductNamesFromRedis(final List<String> productIds) {
		return redisTemplate.opsForValue().multiGet(productIds);
	}

	/**
	 * Replaces missing product names with a default placeholder.
	 *
	 * @param productIds List of product IDs.
	 * @param productNames List of product names retrieved from Redis.
	 * @return List of product names with missing values replaced.
	 */
	private List<String> replaceMissingProductNames(final List<String> productIds, final List<String> productNames) {
		if (productNames == null) {
			return new ArrayList<>(Collections.nCopies(productIds.size(), MISSING_PRODUCT_NAME));
		}

		for (int i = 0; i < productNames.size(); i++) {
			if (productNames.get(i) == null) {
				productNames.set(i, MISSING_PRODUCT_NAME);
			}
		}
		return productNames;
	}
}
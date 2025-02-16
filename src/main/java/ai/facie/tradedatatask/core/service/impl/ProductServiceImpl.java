package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
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
	 * Loads product data from an input stream (CSV file) and stores it in Redis **efficiently**.
	 *
	 * <p>This method now:
	 * - Reads the file **in parallel** using Reactor.
	 * - **Batches inserts** to Redis for better performance.
	 * - Uses **efficient memory management**.</p>
	 *
	 * @param stream The input stream containing product data in CSV format.
	 */
	@Override
	public void loadProductsFromStream(final InputStream stream) {
		log.info("🚀 Starting to load products into Redis...");

		Flux.using(
			() -> new BufferedReader(new InputStreamReader(stream)),
			reader -> Flux.fromStream(reader.lines().skip(START_LINE))
				.parallel()
				.runOn(Schedulers.boundedElastic())
				.map(this::parseProduct)
				.filter(Objects::nonNull)
				.sequential()
				.buffer(BATCH_SIZE)
				.doOnNext(this::batchInsertToRedis)
				.doOnComplete(() -> log.info("Finished loading products into Redis.")),
			reader -> Schedulers.boundedElastic().schedule(() -> {
				try {
					reader.close();
				} catch (Exception e) {
					log.error("❌ Error closing reader", e);
				}
			})
		).blockLast();
	}

	/**
	 * Parses a product record from a line.
	 *
	 * @param line A single line from the file.
	 * @return A key-value pair (productId -> productName) or null if invalid.
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
	 * Stores product entries in Redis **in batch**.
	 *
	 * <p>Uses **pipeline** to reduce the number of calls to Redis.</p>
	 *
	 * @param batch List of product records (productId -> productName).
	 */
	private void batchInsertToRedis(final List<Map.Entry<String, String>> batch) {
		final Map<String, String> productMap = batch.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		redisTemplate.opsForValue().multiSet(productMap);
	}

	/**
	 * Retrieves product names from Redis in batch.
	 *
	 * <p>If a product ID is not found, it returns "Missing Product Name".</p>
	 *
	 * @param productIds List of product IDs.
	 * @return List of product names in the same order.
	 */
	@Override
	public List<String> getProductNamesInBatch(final List<String> productIds) {
		List<String> productNames = redisTemplate.opsForValue().multiGet(productIds);

		if (productNames == null) {
			productNames = new ArrayList<>(Collections.nCopies(productIds.size(), MISSING_PRODUCT_NAME));
		} else {
			for (int i = 0; i < productNames.size(); i++) {
				if (productNames.get(i) == null) {
					productNames.set(i, MISSING_PRODUCT_NAME);
				}
			}
		}

		return productNames;
	}
}
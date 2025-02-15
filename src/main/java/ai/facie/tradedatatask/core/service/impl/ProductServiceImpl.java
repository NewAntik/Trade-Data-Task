package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
@Service
@AllArgsConstructor
public class ProductServiceImpl implements ProductService {
	private static final String MISSING_PRODUCT_NAME = "Missing Product Name";
	private static final int START_LINE = 1;

	private final RedisTemplate<String, String> redisTemplate;

	/**
	 * Loads product data from an input stream (CSV file) and stores it in Redis.
	 *
	 * <p>This method reads product records from the provided input stream, skipping the first line
	 * (header), and then processes each line to store product ID and product name in Redis.
	 *
	 * @param stream The input stream containing product data in CSV format.
	 */
	@Override
	public void loadProductsFromStream(final InputStream stream) {
		log.info("loadProductsFromCSV was called");

		final BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		reader.lines().skip(START_LINE).forEach(this::populateCache);

		log.info("Successfully loaded product data into Redis.");
	}

	/**
	 * Retrieves the product name from Redis based on the provided product ID.
	 *
	 * <p>If the product ID exists in Redis, the corresponding product name is returned.
	 * Otherwise, it returns a default value: {@code "Missing Product Name"}.
	 *
	 * @param productId The ID of the product to look up.
	 * @return The corresponding product name if found, otherwise {@code "Missing Product Name"}.
	 */
	@Override
	public String getProductName(final Long productId) {
		log.info("getProductName was called with product id: {}", productId);

		final String productName = redisTemplate.opsForValue().get(String.valueOf(productId));

		return productName != null ? productName : MISSING_PRODUCT_NAME;
	}

	/**
	 * Stores a product entry in Redis after validating the input CSV record.
	 *
	 * <p>This method processes a single line of the CSV file, extracting the product ID and
	 * product name. If the line does not contain exactly two elements, it is skipped and logged as a warning.
	 *
	 * @param line A single line from the product CSV file (excluding the header).
	 */
	private void populateCache(final String line) {
		final String[] parts = line.split(",");
		if (parts.length == 2) {
			redisTemplate.opsForValue().set(parts[0], parts[1]);
		} else {
			log.warn("Skipping invalid product record: {}", line);
		}
	}
}

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

	@Override
	public void loadProductsFromStream(final InputStream stream) {
		log.info("Starting to load products from stream.");

		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
			processProductStream(reader);
		} catch (final IOException e) {
			log.error("Error reading from input stream", e);
		}
	}

	private void processProductStream(BufferedReader reader) {
		Flux.fromStream(reader.lines().skip(START_LINE))
			.parallel()
			.runOn(Schedulers.boundedElastic())
			.map(this::parseProduct)
			.filter(Objects::nonNull)
			.sequential()
			.buffer(BATCH_SIZE)
			.doOnNext(this::batchInsertToRedis)
			.blockLast();
	}

	private Map.Entry<String, String> parseProduct(final String line) {
		String[] parts = line.split(",");
		if (parts.length == 2) {
			return Map.entry(parts[0], parts[1]);
		} else {
			log.warn("Skipping invalid product record: {}", line);
			return null;
		}
	}

	private void batchInsertToRedis(final List<Map.Entry<String, String>> batch) {
		Map<String, String> productMap = batch.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		redisTemplate.opsForValue().multiSet(productMap);
	}

	@Override
	public List<String> getProductNamesInBatch(final List<String> productIds) {
		List<String> productNames = fetchProductNamesFromRedis(productIds);
		return replaceMissingProductNames(productIds, productNames);
	}

	private List<String> fetchProductNamesFromRedis(List<String> productIds) {
		return redisTemplate.opsForValue().multiGet(productIds);
	}

	private List<String> replaceMissingProductNames(List<String> productIds, List<String> productNames) {
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
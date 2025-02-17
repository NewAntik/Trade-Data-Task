package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeServiceImplTest {

	@Mock
	private ProductService productService;

	@InjectMocks
	private TradeServiceImpl tradeService;

	private static final String VALID_CSV = "date,productName,currency,price\n" +
		"20240101,123,USD,100.50\n" +
		"20240102,456,EUR,200.75\n";

	private static final String INVALID_CSV = "date,productName,currency,price\n" +
		"invalidDate,123,USD,100.50\n" +
		"20240102,invalidId,EUR,200.75\n";

	@Test
	void testEnrichTradesStream_WithValidData() {
		InputStream inputStream = new ByteArrayInputStream(VALID_CSV.getBytes());
		Flux<String> result = tradeService.enrichTradesStream(inputStream);

		when(productService.getProductNamesInBatch(anyList())).thenAnswer(invocation -> {
			List<String> productIds = invocation.getArgument(0);
			return productIds.stream().map(id -> "Product" + id).toList();
		});

		StepVerifier.create(result)
			.expectNext("date,productName,currency,price\n")
			.expectNext("20240101,Product123,USD,100.50\n")
			.expectNext("20240102,Product456,EUR,200.75\n")
			.verifyComplete();
	}

	@Test
	void testEnrichTradesStream_WithInvalidData() {
		InputStream inputStream = new ByteArrayInputStream(INVALID_CSV.getBytes());
		Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext("date,productName,currency,price\n")
			.verifyComplete();
	}

	@Test
	void testEnrichTradesStream_EmptyInput() {
		InputStream inputStream = new ByteArrayInputStream("date,productName,currency,price\n".getBytes());
		Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext("date,productName,currency,price\n") // Expect header
			.expectNextCount(0) // Expect no additional data
			.verifyComplete();
	}

	@Test
	void testEnrichTradesStream_WithLargeBatch() {
		StringBuilder csvBuilder = new StringBuilder("date,productName,currency,price\n");
		for (int i = 0; i < 1500; i++) {
			csvBuilder.append("20240101,").append(i).append(",USD,100.50\n");
		}
		InputStream inputStream = new ByteArrayInputStream(csvBuilder.toString().getBytes());
		Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext("date,productName,currency,price\n")
			.expectNextCount(1500)
			.verifyComplete();
	}
}
package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
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

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TradeServiceImplTest {
	private static final String VALID_CSV = "date,productName,currency,price\n20240101,123,USD,100\n20240102,124,EUR,200";
	private static final String INVALID_CSV = "invalidDate,123,USD,100\n20240102,INVALID_ID,EUR,200";

	@Mock
	private ProductService productService;

	@InjectMocks
	private TradeServiceImpl tradeService;

	@Test
	void testEnrichTradesStream_ValidData() {
		final InputStream inputStream = new ByteArrayInputStream(VALID_CSV.getBytes());
		when(productService.getProductNamesInBatch(List.of("123", "124")))
			.thenReturn(List.of("Product A", "Product B"));

		final Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext(TradeServiceImpl.TABLE_HEADER)
			.expectNext("20240101,Product A,USD,100\n")
			.expectNext("20240102,Product B,EUR,200\n")
			.verifyComplete();
	}

	@Test
	void testEnrichTradesStream_InvalidData() {
		final InputStream inputStream = new ByteArrayInputStream(INVALID_CSV.getBytes());

		final Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext(TradeServiceImpl.TABLE_HEADER)
			.expectError(NullPointerException.class)
			.verify();
	}

	@Test
	void testEnrichTradesStream_EmptyStream() {
		final InputStream inputStream = new ByteArrayInputStream("date,productName,currency,price\n".getBytes());
		final Flux<String> result = tradeService.enrichTradesStream(inputStream);

		StepVerifier.create(result)
			.expectNext(TradeServiceImpl.TABLE_HEADER)
			.verifyComplete();
	}
}
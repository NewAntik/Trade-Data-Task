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

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TradeServiceImplTest {
	private static final Long FIRST_ID = 1L;
	private static final Long SECOND_ID = 2L;
	public static final String TREASURY_BILLS_DOMESTIC = "Treasury Bills Domestic";
	public static final String CORPORATE_BONDS_DOMESTIC = "Corporate Bonds Domestic";

	private static final String VALID_CSV = "date,productId,currency,price\n" +
		"20230101,1,USD,100.25\n" +
		"20230102,2,EUR,200.45\n";

	private static final String INVALID_CSV = "date,productId,currency,price\n" +
		"INVALID_DATE,1,USD,100.25\n" +
		"20230103,INVALID_ID,EUR,300.50\n";

	@Mock
	private ProductService productService;

	@InjectMocks
	private TradeServiceImpl tradeService;

	/**
	 * est case: Successfully processes valid trade records.
	 * Expected: Each trade is enriched with a product name.
	 */
	@Test
	void testEnrichTradesStream_Success() {
		final InputStream stream = new ByteArrayInputStream(VALID_CSV.getBytes());
		final Flux<String> result = tradeService.enrichTradesStream(stream);

		when(productService.getProductName(FIRST_ID)).thenReturn(TREASURY_BILLS_DOMESTIC);
		when(productService.getProductName(SECOND_ID)).thenReturn(CORPORATE_BONDS_DOMESTIC);

		StepVerifier.create(result)
			.expectNext("20230102,Corporate Bonds Domestic,EUR,200.45")
			.expectNext("20230101,Treasury Bills Domestic,USD,100.25")
			.verifyComplete();
	}

	/**
	 * Test case: Skips invalid trade records.
	 * Expected: Invalid records should be filtered out.
	 */
	@Test
	void testEnrichTradesStream_WithInvalidRecords() {
		final InputStream stream = new ByteArrayInputStream(INVALID_CSV.getBytes());
		final Flux<String> result = tradeService.enrichTradesStream(stream);

		StepVerifier.create(result).verifyComplete();
	}

	/**
	 * Test case: Processes a large input stream efficiently.
	 * Expected: Should complete processing without memory issues.
	 */
	@Test
	void testEnrichTradesStream_LargeFile() {
		final StringBuilder largeCsv = getLargeFile();

		mockAllProducts();

		final InputStream stream = new ByteArrayInputStream(largeCsv.toString().getBytes());
		final Flux<String> result = tradeService.enrichTradesStream(stream);

		StepVerifier.create(result)
			.expectNextCount(10000)
			.verifyComplete();
	}

	private void mockAllProducts(){
		for (long id = 1; id <= 5; id++) {
			lenient().when(productService.getProductName(id))
				.thenReturn(id % 2 == 0 ? CORPORATE_BONDS_DOMESTIC : TREASURY_BILLS_DOMESTIC);
		}
	}

	private StringBuilder getLargeFile(){
		final StringBuilder largeCsv = new StringBuilder("date,productId,currency,price\n");
		for (int i = 1; i <= 10000; i++) {
			largeCsv.append("202301").append(String.format("%02d", i % 30 + 1)).append(",")
				.append(i % 5 + 1).append(",USD,").append(100.0 + i).append("\n");
		}

		return largeCsv;
	}
}
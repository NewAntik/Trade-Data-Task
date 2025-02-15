package ai.facie.tradedatatask.core.service.impl;

import ai.facie.tradedatatask.core.model.Product;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductServiceImplTest {

	private static final String VALID_CSV = "productId,productName\n1,Apple Product\n2,Samsung Product\n";
	private static final String INVALID_CSV = "productId,productName\n1,Apple Product\nINVALID_LINE\n2,Samsung Product\n";
	private static final String MISSING_PRODUCT_NAME = "Missing Product Name";
	private static final String APPLE_PRODUCT = "Apple Product";
	private static final String SAMSUNG_PRODUCT = "Samsung Product";
	private static final String FIRST_ID = "1";
	private static final String SECOND_ID = "2";

	@Mock
	private RedisTemplate<String, String> redisTemplate;

	@Mock
	private ValueOperations<String, String> valueOperations;

	@InjectMocks
	private ProductServiceImpl productService;


	@BeforeEach
	void setUp() {
		when(redisTemplate.opsForValue()).thenReturn(valueOperations);
	}

	/**
	 *  Test case: Successfully loads product data from a valid CSV stream.
	 * Expected: Each productId-productName pair should be stored in Redis.
	 */
	@Test
	void testLoadProductsFromStream_Success() {
		InputStream stream = new ByteArrayInputStream(VALID_CSV.getBytes());

		productService.loadProductsFromStream(stream);

		verify(valueOperations).set(FIRST_ID, APPLE_PRODUCT);
		verify(valueOperations).set(SECOND_ID, SAMSUNG_PRODUCT);
		verifyNoMoreInteractions(valueOperations);
	}

	/**
	 *  Test case: Loads product data from a CSV with an invalid line.
	 * Expected: The invalid line should be skipped and only valid lines should be stored.
	 */
	@Test
	void testLoadProductsFromStream_WithInvalidLines() {
		InputStream stream = new ByteArrayInputStream(INVALID_CSV.getBytes());

		productService.loadProductsFromStream(stream);

		verify(valueOperations).set(FIRST_ID, APPLE_PRODUCT);
		verify(valueOperations).set(SECOND_ID, SAMSUNG_PRODUCT);
		verifyNoMoreInteractions(valueOperations);
	}

	/**
	 *  Test case: Retrieves an existing product name from Redis.
	 * Expected: Returns the correct product name.
	 */
	@Test
	void testGetProductName_Found() {
		when(valueOperations.get(FIRST_ID)).thenReturn(APPLE_PRODUCT);

		String productName = productService.getProductName(1L);

		assertEquals(APPLE_PRODUCT, productName);
	}

	/**
	 * Test case: Tries to retrieve a non-existent product from Redis.
	 * Expected: Returns "Missing Product Name".
	 */
	@Test
	void testGetProductName_NotFound() {
		when(valueOperations.get("99")).thenReturn(null);

		String productName = productService.getProductName(99L);

		assertEquals(MISSING_PRODUCT_NAME, productName);
	}
}
package ai.facie.tradedatatask.core.service.impl;

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
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductServiceImplTest {

	private static final String PRODUCT_ID_1 = "id1";
	private static final String PRODUCT_ID_2 = "id2";
	private static final String PRODUCT_ID_3 = "id3";
	private static final String PRODUCT_NAME_A = "Product A";
	private static final String PRODUCT_NAME_B = "Product B";
	private static final String PRODUCT_NAME_C = "Product C";
	private static final String MISSING_PRODUCT_NAME = "Missing Product Name";
	private static final String PRODUCT_DATA = PRODUCT_ID_1 + "," + PRODUCT_NAME_A + "\n" + PRODUCT_ID_2 + "," + PRODUCT_NAME_B;

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

	@Test
	void testLoadProductsFromStream() {
		final InputStream inputStream = new ByteArrayInputStream(PRODUCT_DATA.getBytes());

		productService.loadProductsFromStream(inputStream);

		verify(redisTemplate.opsForValue(), atLeastOnce()).multiSet(anyMap());
	}

	@Test
	void testGetProductNamesInBatch_AllFound() {
		final List<String> productIds = Arrays.asList(PRODUCT_ID_1, PRODUCT_ID_2);
		final List<String> productNames = Arrays.asList(PRODUCT_NAME_A, PRODUCT_NAME_B);

		when(valueOperations.multiGet(productIds)).thenReturn(productNames);

		final List<String> result = productService.getProductNamesInBatch(productIds);
		assertEquals(productNames, result);
	}

	@Test
	void testGetProductNamesInBatch_WithMissingProducts() {
		final List<String> productIds = Arrays.asList(PRODUCT_ID_1, PRODUCT_ID_2, PRODUCT_ID_3);
		final List<String> productNames = Arrays.asList(PRODUCT_NAME_A, null, PRODUCT_NAME_C);

		when(valueOperations.multiGet(productIds)).thenReturn(productNames);

		final List<String> result = productService.getProductNamesInBatch(productIds);
		assertEquals(Arrays.asList(PRODUCT_NAME_A, MISSING_PRODUCT_NAME, PRODUCT_NAME_C), result);
	}

	@Test
	void testGetProductNamesInBatch_AllMissing() {
		final List<String> productIds = Arrays.asList(PRODUCT_ID_1, PRODUCT_ID_2);

		when(valueOperations.multiGet(productIds)).thenReturn(null);

		final List<String> result = productService.getProductNamesInBatch(productIds);
		assertEquals(Arrays.asList(MISSING_PRODUCT_NAME, MISSING_PRODUCT_NAME), result);
	}
}
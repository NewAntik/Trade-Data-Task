package ai.facie.tradedatatask.controller;

import ai.facie.tradedatatask.core.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class ProductControllerTest {
	private static final String URL = "/api/v1/products";
	private static final String SUCCESSFUL_UPLOAD_MESSAGE = "Product data loaded successfully into Redis.";
	private static final String FAILED_UPLOAD_MESSAGE = "Upload failed: The file is empty.";

	private MockMvc mockMvc;

	@Mock
	private ProductService productService;

	@InjectMocks
	private ProductController productController;

	@BeforeEach
	void setUp() {
		this.mockMvc = MockMvcBuilders.standaloneSetup(productController).build();
	}

	/**
	 * Test case: Successfully uploads a product CSV file.
	 * Expected: Returns `200 OK` with success message.
	 */
	@Test
	void testLoadProducts_Success() throws Exception {
		final MockMultipartFile file = getNotEmptyFile();


		doNothing().when(productService).loadProductsFromStream(any());

		mockMvc.perform(multipart(URL)
				.file(file)
				.contentType(MediaType.MULTIPART_FORM_DATA))
			.andExpect(status().isOk())
			.andExpect(content().string(SUCCESSFUL_UPLOAD_MESSAGE));
	}

	/**
	 * Test case: Uploading an empty file.
	 * Expected: Returns `400 Bad Request` with appropriate message.
	 */
	@Test
	void testLoadProducts_EmptyFile() throws Exception {
		final MockMultipartFile emptyFile = getEmptyFile();

		mockMvc.perform(multipart(URL)
				.file(emptyFile)
				.contentType(MediaType.MULTIPART_FORM_DATA))
			.andExpect(status().isBadRequest())
			.andExpect(content().string(FAILED_UPLOAD_MESSAGE));
	}

	private MockMultipartFile getEmptyFile() {
		return new MockMultipartFile(
			"file",
			"empty.csv",
			MediaType.TEXT_PLAIN_VALUE,
			new byte[0]
		);
	}

	private MockMultipartFile getNotEmptyFile() {
		return new MockMultipartFile(
			"file",
			"products.csv",
			MediaType.TEXT_PLAIN_VALUE,
			"productId,productName\n1,Test Product\n2,Another Product".getBytes()
		);
	}
}
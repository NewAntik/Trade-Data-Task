package ai.facie.tradedatatask.controller;

import ai.facie.tradedatatask.core.service.ProductService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/products")
public class ProductController {

	private final ProductService productService;

	/**
	 * Uploads a product CSV file and stores its data in Redis.
	 *
	 * <p>This method reads a CSV file containing product data, processes its content,
	 * and loads productId-to-productName mappings into Redis.</p>
	 *
	 * @param file The uploaded CSV file containing product data.
	 * @return ResponseEntity with a success message or an error if the file is invalid.
	 * @throws IOException if an error occurs while reading the file.
	 */
	@PostMapping(consumes = "multipart/form-data")
	public ResponseEntity<String> loadProducts(@RequestParam("file") final MultipartFile file) throws IOException {
		log.info("loadProducts was called with file name: {}", file.getOriginalFilename());

		if (file.isEmpty()) {
			return ResponseEntity.badRequest().body("Upload failed: The file is empty.");
		}
		final Instant start = Instant.now();
		productService.loadProductsFromStream(file.getInputStream());
		final Instant end = Instant.now();

		log.info("File processing completed in {} ms", Duration.between(start, end).toMillis());
		productService.loadProductsFromStream(file.getInputStream());

		return ResponseEntity.ok("Product data loaded successfully into Redis.");
	}
}

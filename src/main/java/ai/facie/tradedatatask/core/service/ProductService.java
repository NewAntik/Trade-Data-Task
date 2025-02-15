package ai.facie.tradedatatask.core.service;

import java.io.InputStream;

public interface ProductService {

	void loadProductsFromStream(InputStream stream);

	String getProductName(Long productId);
}

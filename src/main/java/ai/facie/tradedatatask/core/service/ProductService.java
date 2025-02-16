package ai.facie.tradedatatask.core.service;

import java.io.InputStream;
import java.util.List;

public interface ProductService {

	void loadProductsFromStream(InputStream stream);

	String getProductName(Long productId);

	List<String> getProductNamesInBatch(List<String> productIds);
}

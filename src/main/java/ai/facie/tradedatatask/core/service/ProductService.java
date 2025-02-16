package ai.facie.tradedatatask.core.service;

import java.io.InputStream;
import java.util.List;

public interface ProductService {

	void loadProductsFromStream(InputStream stream);

	List<String> getProductNamesInBatch(List<String> productIds);
}

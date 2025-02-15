package ai.facie.tradedatatask.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Trade {

	private String date;

	private String productName;

	private String currency;

	private BigDecimal price;

}

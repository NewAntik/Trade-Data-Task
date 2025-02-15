package ai.facie.tradedatatask.controller.util;

import ai.facie.tradedatatask.core.model.Trade;

import java.util.List;

public class ResponseFormatter {
	private static final String TABLE_HEADER = "date,productName,currency,price\n";

	/**
	 * Converts a list of {@link Trade} into a structured table format as a string.
	 *
	 * <p>This method generates a tabular representation of trade data, formatted as a string.
	 * Each trade is represented as a comma-separated row, with the first row containing
	 * column headers.</p>
	 *
	 * <p>The resulting table includes:</p>
	 * <ul>
	 *     <li>A predefined table header ({@code TABLE_HEADER}).</li>
	 *     <li>Each trade formatted as: {@code date,productName,currency,price}.</li>
	 *     <li>Each row ending with a newline character.</li>
	 * </ul>
	 *
	 * @param trades The list of {@link Trade} to format.
	 * @return A formatted string representing the trade data in table format.
	 */
	public static String format(final List<Trade> trades) {
		final StringBuilder sb = new StringBuilder(TABLE_HEADER);

		trades.forEach(trade -> sb.append(trade.getDate()).append(",")
			.append(trade.getProductName()).append(",")
			.append(trade.getCurrency()).append(",")
			.append(trade.getPrice()).append("\n"));

		return sb.toString();
	}
}

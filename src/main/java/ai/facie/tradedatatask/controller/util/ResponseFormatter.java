package ai.facie.tradedatatask.controller.util;

import java.util.List;

public class ResponseFormatter {
	private static final String TABLE_HEADER = "date,productName,currency,price\n";

	/**
	 * Converts a list of trade strings into a structured table format.
	 *
	 * <p>This method ensures that each trade record is placed on a new line and
	 * that the output properly follows a tabular format.</p>
	 *
	 * @param trades The list of trade strings (already formatted) to include in the table.
	 * @return A formatted string representing the trade data in table format.
	 */
	public static String format(final List<String> trades) {
		if (trades.isEmpty()) {
			return TABLE_HEADER;
		}

		final StringBuilder sb = new StringBuilder(TABLE_HEADER);

		for (final String trade : trades) {
			sb.append(trade).append("\n");
		}

		return sb.toString();
	}
}

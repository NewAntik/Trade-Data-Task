package ai.facie.tradedatatask.controller;

import ai.facie.tradedatatask.core.service.TradeService;
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
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class TradeControllerTest {
	private static final String URL = "/api/v1/trades";

	private MockMvc mockMvc;

	@Mock
	private TradeService tradeService;

	@InjectMocks
	private TradeController tradeController;

	@BeforeEach
	void setUp() {
		this.mockMvc = MockMvcBuilders.standaloneSetup(tradeController).build();
	}

	@Test
	void testEnrichTrades_Success() throws Exception {
		final MockMultipartFile file = getNotEmptyFile();

		final List<String> processedTrades = List.of(
			"20230101,Commodity Swaps 1,USD,100.25",
			"20230102,Commodity Swaps,EUR,200.50"
		);
		when(tradeService.enrichTradesStream(any(InputStream.class)))
			.thenReturn(Flux.fromIterable(processedTrades));

		mockMvc.perform(multipart(URL)
				.file(file)
				.contentType(MediaType.MULTIPART_FORM_DATA))
			.andExpect(status().isOk());
	}

	@Test
	void testEnrichTrades_EmptyFile() throws Exception {
		final MockMultipartFile emptyFile = getEmptyFile();

		mockMvc.perform(multipart(URL)
				.file(emptyFile)
				.contentType(MediaType.MULTIPART_FORM_DATA))
			.andExpect(status().isOk());
	}

	private MockMultipartFile getNotEmptyFile() {
		return new MockMultipartFile(
			"file",
			"trades.csv",
			MediaType.TEXT_PLAIN_VALUE,
			"date,productId,currency,price\n20230101,1,USD,100.25\n20230102,2,EUR,200.50".getBytes()
		);
	}

	private MockMultipartFile getEmptyFile() {
		return new MockMultipartFile(
			"file",
			"empty.csv",
			MediaType.TEXT_PLAIN_VALUE,
			new byte[0]
		);
	}

}
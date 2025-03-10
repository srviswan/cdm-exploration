package com.example.demo.controller;

import com.example.demo.service.TradeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DemoControllerTest {

    @Mock
    private TradeService tradeService;

    @InjectMocks
    private DemoController demoController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void hello_ShouldReturnHelloMessage() {
        ResponseEntity<String> response = demoController.hello();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Hello from Spring Boot 3!", response.getBody());
    }

    @Test
    void echo_ShouldReturnEchoMessage() {
        String message = "test message";
        ResponseEntity<String> response = demoController.echo(message);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Echo: " + message, response.getBody());
    }

    @Test
    void sampleTrade_ShouldReturnTradeFromService() {
        Object mockTrade = new Object();
        when(tradeService.readTradeFromUrl(anyString())).thenReturn(mockTrade);

        ResponseEntity<Object> response = demoController.sampleTrade();
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTrade, response.getBody());
        verify(tradeService).readTradeFromUrl(anyString());
    }

    @Test
    void readTrade_ShouldReturnTradeFromService() {
        String testUrl = "https://github.com/finos/common-domain-model/blob/b06913fe28e55a23878446d9879dfde328586b0e/rosetta-source/src/main/resources/result-json-files/fpml-5-13/products/equity-swaps/eqs-ex01-single-underlyer-execution-long-form.json";
        Object mockTrade = new Object();
        when(tradeService.readTradeFromUrl(testUrl)).thenReturn(mockTrade);

        ResponseEntity<Object> response = demoController.readTrade(testUrl);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTrade, response.getBody());
        verify(tradeService).readTradeFromUrl(testUrl);
    }

    @Test
    void readTrade_WhenServiceThrowsException_ShouldPropagateException() {
        String testUrl = "https://github.com/finos/common-domain-model/blob/b06913fe28e55a23878446d9879dfde328586b0e/rosetta-source/src/main/resources/result-json-files/fpml-5-13/products/equity-swaps/eqs-ex01-single-underlyer-execution-long-form.json";
        when(tradeService.readTradeFromUrl(testUrl)).thenThrow(new RuntimeException("Test error"));

        assertThrows(RuntimeException.class, () -> demoController.readTrade(testUrl));
        verify(tradeService).readTradeFromUrl(testUrl);
    }
}

package com.example.demo.controller;

import com.example.demo.service.TradeService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class DemoController {
    private final TradeService tradeService;
    private static final String SINGLE_STOCK_TRADE_JSON_URL = "https://raw.githubusercontent.com/finos/common-domain-model/master/rosetta-source/src/main/resources/result-json-files/fpml-5-13/products/equity-swaps/eqs-ex01-single-underlyer-execution-long-form.json";

    public DemoController(TradeService tradeService) {
        this.tradeService = tradeService;
    }

    @GetMapping("/hello")
    public ResponseEntity<String> hello() {
        return ResponseEntity.ok("Hello from Spring Boot 3!");
    }

    @PostMapping("/echo")
    public ResponseEntity<String> echo(@RequestBody String message) {
        return ResponseEntity.ok("Echo: " + message);
    }

    @GetMapping("/sample-trade")
    public ResponseEntity<Object> sampleTrade() {
        Object tradeJson = tradeService.readTradeFromUrl(SINGLE_STOCK_TRADE_JSON_URL);
        return ResponseEntity.ok(tradeJson);
    }

    @GetMapping("/trade")
    public ResponseEntity<Object> readTrade(@RequestParam String url) {
        Object trade = tradeService.readTradeFromUrl(url);
        return ResponseEntity.ok(trade);
    }
}

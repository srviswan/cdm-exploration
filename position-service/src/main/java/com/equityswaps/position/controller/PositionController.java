package com.equityswaps.position.controller;

import com.equityswaps.common.domain.Position;
import com.equityswaps.position.service.PositionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Position REST Controller.
 * 
 * Provides reactive endpoints for position management operations
 * with Java 23 pattern matching and virtual threads support.
 */
@RestController
@RequestMapping("/api/v1/positions")
@RequiredArgsConstructor
@Slf4j
@Validated
@Tag(name = "Position Management", description = "APIs for equity swap position management")
public class PositionController {

    private final PositionService positionService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get all positions", description = "Retrieve all positions with optional filtering")
    public Flux<Position> getAllPositions(
            @Parameter(description = "Portfolio ID filter")
            @RequestParam(required = false) String portfolioId,
            @Parameter(description = "Instrument symbol filter")
            @RequestParam(required = false) String symbol) {
        
        log.info("Fetching positions - portfolioId: {}, symbol: {}", portfolioId, symbol);
        
        return switch (portfolioId != null ? "portfolio" : symbol != null ? "symbol" : "all") {
            case "portfolio" -> positionService.getPositionsByPortfolio(portfolioId);
            case "symbol" -> positionService.getPositionsBySymbol(symbol);
            case "all" -> positionService.getAllPositions();
            default -> Flux.empty();
        };
    }

    @GetMapping(value = "/{positionId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get position by ID", description = "Retrieve a specific position")
    public Mono<Position> getPosition(
            @Parameter(description = "Position ID", required = true)
            @PathVariable @NotBlank String positionId) {
        
        log.info("Fetching position: {}", positionId);
        return positionService.getPositionById(positionId);
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Create position", description = "Create a new position")
    public Mono<Position> createPosition(@Valid @RequestBody Position position) {
        log.info("Creating position: {}", position.positionId());
        return positionService.createPosition(position);
    }

    @PutMapping(value = "/{positionId}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Update position", description = "Update an existing position")
    public Mono<Position> updatePosition(
            @Parameter(description = "Position ID", required = true)
            @PathVariable @NotBlank String positionId,
            @Valid @RequestBody Position position) {
        
        log.info("Updating position: {}", positionId);
        return positionService.updatePosition(positionId, position);
    }

    @PatchMapping(value = "/{positionId}/market-data", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Update market data", description = "Update position with current market data")
    public Mono<Position> updateMarketData(
            @Parameter(description = "Position ID", required = true)
            @PathVariable @NotBlank String positionId,
            @Parameter(description = "Current price", required = true)
            @RequestParam BigDecimal currentPrice) {
        
        log.info("Updating market data for position: {} with price: {}", positionId, currentPrice);
        return positionService.updateMarketData(positionId, currentPrice, LocalDateTime.now());
    }

    @GetMapping(value = "/{positionId}/pnl", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get P&L details", description = "Get detailed P&L breakdown for a position")
    public Mono<PnLResponse> getPositionPnL(
            @Parameter(description = "Position ID", required = true)
            @PathVariable @NotBlank String positionId) {
        
        log.info("Fetching P&L for position: {}", positionId);
        return positionService.calculatePnL(positionId);
    }

    @GetMapping(value = "/portfolio/{portfolioId}/summary", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get portfolio summary", description = "Get aggregated portfolio P&L summary")
    public Mono<PortfolioSummary> getPortfolioSummary(
            @Parameter(description = "Portfolio ID", required = true)
            @PathVariable @NotBlank String portfolioId) {
        
        log.info("Fetching portfolio summary: {}", portfolioId);
        return positionService.getPortfolioSummary(portfolioId);
    }

    @DeleteMapping("/{positionId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Delete position", description = "Close and delete a position")
    public Mono<Void> deletePosition(
            @Parameter(description = "Position ID", required = true)
            @PathVariable @NotBlank String positionId) {
        
        log.info("Deleting position: {}", positionId);
        return positionService.closePosition(positionId);
    }

    @GetMapping(value = "/real-time", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Real-time position updates", description = "Stream real-time position updates")
    public Flux<Position> getRealTimePositions(
            @Parameter(description = "Portfolio ID filter")
            @RequestParam(required = false) String portfolioId) {
        
        log.info("Starting real-time position stream for portfolio: {}", portfolioId);
        return positionService.getPositionUpdatesStream(portfolioId);
    }

    /**
     * P&L Response DTO with Java 23 record.
     */
    public record PnLResponse(
            String positionId,
            BigDecimal unrealizedPnL,
            BigDecimal realizedPnL,
            BigDecimal totalPnL,
            BigDecimal marketValue,
            BigDecimal costBasis,
            LocalDateTime calculatedAt
    ) {}

    /**
     * Portfolio Summary DTO with Java 23 record.
     */
    public record PortfolioSummary(
            String portfolioId,
            int totalPositions,
            BigDecimal totalMarketValue,
            BigDecimal totalUnrealizedPnL,
            BigDecimal totalRealizedPnL,
            BigDecimal totalPnL,
            BigDecimal exposure,
            LocalDateTime calculatedAt
    ) {}
}
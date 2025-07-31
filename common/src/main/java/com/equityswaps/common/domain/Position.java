package com.equityswaps.common.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Position domain model aligned with FINOS CDM Position specification.
 * Represents an aggregated position for equity swaps with P&L calculation.
 */
@Data
@Builder
@Jacksonized
public record Position(
    @JsonProperty("positionId")
    @NotNull
    @Size(min = 1, max = 100)
    String positionId,
    
    @JsonProperty("portfolioId") 
    @NotNull
    @Size(min = 1, max = 100)
    String portfolioId,
    
    @JsonProperty("instrument")
    @NotNull
    EquitySwapInstrument instrument,
    
    @JsonProperty("quantity")
    @NotNull
    BigDecimal quantity,
    
    @JsonProperty("averageCost")
    @NotNull
    BigDecimal averageCost,
    
    @JsonProperty("currentPrice")
    BigDecimal currentPrice,
    
    @JsonProperty("marketValue")
    BigDecimal marketValue,
    
    @JsonProperty("unrealizedPnL")
    BigDecimal unrealizedPnL,
    
    @JsonProperty("realizedPnL")
    BigDecimal realizedPnL,
    
    @JsonProperty("totalPnL")
    BigDecimal totalPnL,
    
    @JsonProperty("lots")
    List<String> lotIds,
    
    @JsonProperty("riskMetrics")
    Map<String, BigDecimal> riskMetrics,
    
    @JsonProperty("createdAt")
    @NotNull
    LocalDateTime createdAt,
    
    @JsonProperty("updatedAt")
    @NotNull
    LocalDateTime updatedAt,
    
    @JsonProperty("version")
    @NotNull
    Long version
) {
    
    /**
     * Calculates the current market value based on quantity and current price.
     */
    public BigDecimal calculateMarketValue() {
        if (currentPrice != null && quantity != null) {
            return quantity.multiply(currentPrice);
        }
        return BigDecimal.ZERO;
    }
    
    /**
     * Calculates unrealized P&L based on current market value and cost basis.
     */
    public BigDecimal calculateUnrealizedPnL() {
        if (currentPrice != null && averageCost != null && quantity != null) {
            BigDecimal costBasis = quantity.multiply(averageCost);
            BigDecimal currentValue = calculateMarketValue();
            return currentValue.subtract(costBasis);
        }
        return BigDecimal.ZERO;
    }
    
    /**
     * Calculates total P&L (realized + unrealized).
     */
    public BigDecimal calculateTotalPnL() {
        BigDecimal realized = realizedPnL != null ? realizedPnL : BigDecimal.ZERO;
        BigDecimal unrealized = calculateUnrealizedPnL();
        return realized.add(unrealized);
    }
    
    /**
     * Creates a new Position with updated market data.
     */
    public Position withMarketData(BigDecimal newPrice, LocalDateTime timestamp) {
        return Position.builder()
            .positionId(this.positionId)
            .portfolioId(this.portfolioId)
            .instrument(this.instrument)
            .quantity(this.quantity)
            .averageCost(this.averageCost)
            .currentPrice(newPrice)
            .marketValue(calculateMarketValue())
            .unrealizedPnL(calculateUnrealizedPnL())
            .realizedPnL(this.realizedPnL)
            .totalPnL(calculateTotalPnL())
            .lotIds(this.lotIds)
            .riskMetrics(this.riskMetrics)
            .createdAt(this.createdAt)
            .updatedAt(timestamp)
            .version(this.version + 1)
            .build();
    }
    
    /**
     * Checks if the position is long (positive quantity).
     */
    public boolean isLong() {
        return quantity != null && quantity.compareTo(BigDecimal.ZERO) > 0;
    }
    
    /**
     * Checks if the position is short (negative quantity).
     */
    public boolean isShort() {
        return quantity != null && quantity.compareTo(BigDecimal.ZERO) < 0;
    }
    
    /**
     * Checks if the position is flat (zero quantity).
     */
    public boolean isFlat() {
        return quantity != null && quantity.compareTo(BigDecimal.ZERO) == 0;
    }
}
package com.equityswaps.common.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Lot domain model for lot-level position tracking.
 * Supports FIFO, LIFO, and average cost accounting methods.
 */
@Builder
@Jacksonized
public record Lot(
    @JsonProperty("lotId")
    @NotNull
    @Size(min = 1, max = 100)
    String lotId,
    
    @JsonProperty("positionId")
    @NotNull
    @Size(min = 1, max = 100)
    String positionId,
    
    @JsonProperty("tradeId")
    @NotNull
    @Size(min = 1, max = 100)
    String tradeId,
    
    @JsonProperty("instrument")
    @NotNull
    EquitySwapInstrument instrument,
    
    @JsonProperty("originalQuantity")
    @NotNull
    BigDecimal originalQuantity,
    
    @JsonProperty("remainingQuantity")
    @NotNull
    BigDecimal remainingQuantity,
    
    @JsonProperty("costBasis")
    @NotNull
    BigDecimal costBasis,
    
    @JsonProperty("unitCost")
    @NotNull
    BigDecimal unitCost,
    
    @JsonProperty("currentPrice")
    BigDecimal currentPrice,
    
    @JsonProperty("unrealizedPnL")
    BigDecimal unrealizedPnL,
    
    @JsonProperty("realizedPnL")
    BigDecimal realizedPnL,
    
    @JsonProperty("accountingMethod")
    @NotNull
    AccountingMethod accountingMethod,
    
    @JsonProperty("openDate")
    @NotNull
    LocalDateTime openDate,
    
    @JsonProperty("lastUpdated")
    @NotNull
    LocalDateTime lastUpdated,
    
    @JsonProperty("status")
    @NotNull
    LotStatus status,
    
    @JsonProperty("tags")
    Map<String, String> tags,
    
    @JsonProperty("version")
    @NotNull
    Long version
) {
    
    /**
     * Accounting method enumeration.
     */
    public enum AccountingMethod {
        FIFO,  // First In, First Out
        LIFO,  // Last In, First Out
        AVERAGE_COST,
        SPECIFIC_IDENTIFICATION
    }
    
    /**
     * Lot status enumeration.
     */
    public enum LotStatus {
        OPEN,      // Lot is active and has remaining quantity
        CLOSED,    // Lot is fully sold/closed
        PARTIAL,   // Lot is partially sold
        SUSPENDED, // Lot is temporarily suspended
        CORPORATE_ACTION // Lot affected by corporate action
    }
    
    /**
     * Calculates the current market value of the remaining quantity.
     */
    public BigDecimal calculateMarketValue() {
        if (currentPrice != null && remainingQuantity != null) {
            return remainingQuantity.multiply(currentPrice);
        }
        return BigDecimal.ZERO;
    }
    
    /**
     * Calculates unrealized P&L for the remaining quantity.
     */
    public BigDecimal calculateUnrealizedPnL() {
        if (currentPrice != null && unitCost != null && remainingQuantity != null) {
            BigDecimal marketValue = calculateMarketValue();
            BigDecimal costBasisRemaining = remainingQuantity.multiply(unitCost);
            return marketValue.subtract(costBasisRemaining);
        }
        return BigDecimal.ZERO;
    }
    
    /**
     * Calculates the total P&L (realized + unrealized).
     */
    public BigDecimal calculateTotalPnL() {
        BigDecimal realized = realizedPnL != null ? realizedPnL : BigDecimal.ZERO;
        BigDecimal unrealized = calculateUnrealizedPnL();
        return realized.add(unrealized);
    }
    
    /**
     * Checks if the lot is fully closed.
     */
    public boolean isClosed() {
        return status == LotStatus.CLOSED || 
               (remainingQuantity != null && remainingQuantity.compareTo(BigDecimal.ZERO) == 0);
    }
    
    /**
     * Checks if the lot has remaining quantity.
     */
    public boolean hasRemainingQuantity() {
        return remainingQuantity != null && remainingQuantity.compareTo(BigDecimal.ZERO) > 0;
    }
    
    /**
     * Calculates the percentage of the lot that has been sold.
     */
    public BigDecimal getSoldPercentage() {
        if (originalQuantity != null && originalQuantity.compareTo(BigDecimal.ZERO) > 0) {
            BigDecimal soldQuantity = originalQuantity.subtract(remainingQuantity);
            return soldQuantity.divide(originalQuantity, 4, BigDecimal.ROUND_HALF_UP)
                              .multiply(BigDecimal.valueOf(100));
        }
        return BigDecimal.ZERO;
    }
    
    /**
     * Creates a new lot with updated market data.
     */
    public Lot withMarketData(BigDecimal newPrice, LocalDateTime timestamp) {
        return Lot.builder()
            .lotId(this.lotId)
            .positionId(this.positionId)
            .tradeId(this.tradeId)
            .instrument(this.instrument)
            .originalQuantity(this.originalQuantity)
            .remainingQuantity(this.remainingQuantity)
            .costBasis(this.costBasis)
            .unitCost(this.unitCost)
            .currentPrice(newPrice)
            .unrealizedPnL(calculateUnrealizedPnL())
            .realizedPnL(this.realizedPnL)
            .accountingMethod(this.accountingMethod)
            .openDate(this.openDate)
            .lastUpdated(timestamp)
            .status(this.status)
            .tags(this.tags)
            .version(this.version + 1)
            .build();
    }
    
    /**
     * Creates a lot after a partial sale.
     */
    public Lot afterPartialSale(BigDecimal soldQuantity, BigDecimal salePrice, 
                                BigDecimal realizedGainLoss, LocalDateTime timestamp) {
        BigDecimal newRemainingQuantity = remainingQuantity.subtract(soldQuantity);
        BigDecimal newRealizedPnL = (realizedPnL != null ? realizedPnL : BigDecimal.ZERO)
                                   .add(realizedGainLoss);
        
        LotStatus newStatus = newRemainingQuantity.compareTo(BigDecimal.ZERO) == 0 
                             ? LotStatus.CLOSED 
                             : LotStatus.PARTIAL;
        
        return Lot.builder()
            .lotId(this.lotId)
            .positionId(this.positionId)
            .tradeId(this.tradeId)
            .instrument(this.instrument)
            .originalQuantity(this.originalQuantity)
            .remainingQuantity(newRemainingQuantity)
            .costBasis(this.costBasis)
            .unitCost(this.unitCost)
            .currentPrice(this.currentPrice)
            .unrealizedPnL(calculateUnrealizedPnL())
            .realizedPnL(newRealizedPnL)
            .accountingMethod(this.accountingMethod)
            .openDate(this.openDate)
            .lastUpdated(timestamp)
            .status(newStatus)
            .tags(this.tags)
            .version(this.version + 1)
            .build();
    }
}
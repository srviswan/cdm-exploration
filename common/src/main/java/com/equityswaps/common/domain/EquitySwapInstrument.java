package com.equityswaps.common.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;

/**
 * Equity Swap Instrument domain model aligned with FINOS CDM Instrument specification.
 * Represents the underlying instrument details for equity swap positions.
 */
@Builder
@Jacksonized
public record EquitySwapInstrument(
    @JsonProperty("instrumentId")
    @NotNull
    @Size(min = 1, max = 100)
    String instrumentId,
    
    @JsonProperty("symbol")
    @NotNull
    @Size(min = 1, max = 20)
    String symbol,
    
    @JsonProperty("isin")
    @Size(min = 12, max = 12)
    String isin,
    
    @JsonProperty("cusip")
    @Size(min = 9, max = 9)
    String cusip,
    
    @JsonProperty("sedol")
    @Size(min = 7, max = 7)
    String sedol,
    
    @JsonProperty("ric")
    @Size(min = 1, max = 50)
    String ric,
    
    @JsonProperty("bloomberg")
    @Size(min = 1, max = 50)
    String bloomberg,
    
    @JsonProperty("exchange")
    @NotNull
    @Size(min = 1, max = 10)
    String exchange,
    
    @JsonProperty("currency")
    @NotNull
    @Size(min = 3, max = 3)
    String currency,
    
    @JsonProperty("instrumentType")
    @NotNull
    InstrumentType instrumentType,
    
    @JsonProperty("underlyingAsset")
    @NotNull
    UnderlyingAsset underlyingAsset,
    
    @JsonProperty("notionalAmount")
    BigDecimal notionalAmount,
    
    @JsonProperty("multiplier")
    BigDecimal multiplier,
    
    @JsonProperty("maturityDate")
    LocalDate maturityDate,
    
    @JsonProperty("tradeDate")
    LocalDate tradeDate,
    
    @JsonProperty("settlementDate")
    LocalDate settlementDate,
    
    @JsonProperty("resetFrequency")
    String resetFrequency,
    
    @JsonProperty("paymentFrequency")
    String paymentFrequency,
    
    @JsonProperty("dayCountConvention")
    String dayCountConvention,
    
    @JsonProperty("businessDayConvention")
    String businessDayConvention,
    
    @JsonProperty("riskFactors")
    Map<String, BigDecimal> riskFactors,
    
    @JsonProperty("contractSpecifications")
    Map<String, Object> contractSpecifications
) {
    
    /**
     * Instrument type enumeration aligned with CDM taxonomy.
     */
    public enum InstrumentType {
        EQUITY_SWAP,
        TOTAL_RETURN_SWAP,
        PRICE_RETURN_SWAP,
        DIVIDEND_SWAP,
        VARIANCE_SWAP,
        VOLATILITY_SWAP
    }
    
    /**
     * Underlying asset details for the equity swap.
     */
    @Builder
    @Jacksonized
    public record UnderlyingAsset(
        @JsonProperty("assetId")
        @NotNull
        String assetId,
        
        @JsonProperty("symbol")
        @NotNull
        String symbol,
        
        @JsonProperty("name")
        @NotNull
        String name,
        
        @JsonProperty("assetClass")
        @NotNull
        AssetClass assetClass,
        
        @JsonProperty("sector")
        String sector,
        
        @JsonProperty("industry")
        String industry,
        
        @JsonProperty("country")
        String country,
        
        @JsonProperty("region")
        String region,
        
        @JsonProperty("marketCap")
        BigDecimal marketCap,
        
        @JsonProperty("sharesOutstanding")
        BigDecimal sharesOutstanding,
        
        @JsonProperty("averageDailyVolume")
        BigDecimal averageDailyVolume
    ) {
        
        /**
         * Asset class enumeration.
         */
        public enum AssetClass {
            EQUITY,
            INDEX,
            ETF,
            REIT,
            ADR,
            COMMODITY,
            CURRENCY,
            INTEREST_RATE
        }
    }
    
    /**
     * Calculates the effective notional amount based on multiplier.
     */
    public BigDecimal getEffectiveNotional() {
        if (notionalAmount != null && multiplier != null) {
            return notionalAmount.multiply(multiplier);
        }
        return notionalAmount != null ? notionalAmount : BigDecimal.ZERO;
    }
    
    /**
     * Checks if the instrument has matured.
     */
    public boolean isMatured() {
        return maturityDate != null && maturityDate.isBefore(LocalDate.now());
    }
    
    /**
     * Gets the unique instrument identifier (prefers ISIN, falls back to symbol).
     */
    public String getUniqueIdentifier() {
        if (isin != null && !isin.isEmpty()) {
            return isin;
        }
        if (cusip != null && !cusip.isEmpty()) {
            return cusip;
        }
        return symbol;
    }
    
    /**
     * Creates a composite key for the instrument.
     */
    public String getCompositeKey() {
        return String.format("%s:%s:%s", 
            getUniqueIdentifier(), 
            exchange, 
            currency);
    }
}
package com.equityswaps.common.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Cashflow domain model aligned with FINOS CDM Cashflow specification.
 * Represents cashflows generated from equity swap positions.
 */
@Builder
@Jacksonized
public record Cashflow(
    @JsonProperty("cashflowId")
    @NotNull
    @Size(min = 1, max = 100)
    String cashflowId,
    
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
    
    @JsonProperty("cashflowType")
    @NotNull
    CashflowType cashflowType,
    
    @JsonProperty("amount")
    @NotNull
    BigDecimal amount,
    
    @JsonProperty("currency")
    @NotNull
    @Size(min = 3, max = 3)
    String currency,
    
    @JsonProperty("paymentDate")
    @NotNull
    LocalDate paymentDate,
    
    @JsonProperty("accrualStartDate")
    LocalDate accrualStartDate,
    
    @JsonProperty("accrualEndDate")
    LocalDate accrualEndDate,
    
    @JsonProperty("dayCountFraction")
    BigDecimal dayCountFraction,
    
    @JsonProperty("notionalAmount")
    BigDecimal notionalAmount,
    
    @JsonProperty("rate")
    BigDecimal rate,
    
    @JsonProperty("spread")
    BigDecimal spread,
    
    @JsonProperty("multiplier")
    BigDecimal multiplier,
    
    @JsonProperty("adjustmentFactor")
    BigDecimal adjustmentFactor,
    
    @JsonProperty("discountFactor")
    BigDecimal discountFactor,
    
    @JsonProperty("presentValue")
    BigDecimal presentValue,
    
    @JsonProperty("status")
    @NotNull
    CashflowStatus status,
    
    @JsonProperty("calculationMethod")
    String calculationMethod,
    
    @JsonProperty("businessDayConvention")
    String businessDayConvention,
    
    @JsonProperty("calendar")
    String calendar,
    
    @JsonProperty("resetDate")
    LocalDate resetDate,
    
    @JsonProperty("fixingDate")
    LocalDate fixingDate,
    
    @JsonProperty("settlementDate")
    LocalDate settlementDate,
    
    @JsonProperty("payerParty")
    String payerParty,
    
    @JsonProperty("receiverParty")
    String receiverParty,
    
    @JsonProperty("calculationDetails")
    Map<String, Object> calculationDetails,
    
    @JsonProperty("riskFactors")
    Map<String, BigDecimal> riskFactors,
    
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
     * Cashflow type enumeration aligned with CDM taxonomy.
     */
    public enum CashflowType {
        EQUITY_RETURN,          // Equity performance return
        DIVIDEND,               // Dividend payment
        INTEREST,               // Interest payment (funding leg)
        PRINCIPAL,              // Principal payment
        FEE,                    // Fee payment
        CORPORATE_ACTION,       // Corporate action adjustment
        MARGIN,                 // Margin call
        SETTLEMENT,             // Settlement payment
        COLLATERAL_ADJUSTMENT,  // Collateral adjustment
        BREAKAGE,               // Early termination fee
        PREMIUM                 // Option premium
    }
    
    /**
     * Cashflow status enumeration.
     */
    public enum CashflowStatus {
        PROJECTED,              // Future projected cashflow
        CONFIRMED,              // Confirmed but not paid
        SETTLED,                // Paid/settled
        CANCELLED,              // Cancelled
        DISPUTED,               // Under dispute
        PENDING_APPROVAL,       // Awaiting approval
        FAILED,                 // Payment failed
        PARTIALLY_SETTLED       // Partially paid
    }
    
    /**
     * Calculates the present value using the discount factor.
     */
    public BigDecimal calculatePresentValue() {
        if (amount != null && discountFactor != null) {
            return amount.multiply(discountFactor);
        }
        return amount;
    }
    
    /**
     * Calculates the effective amount including adjustments.
     */
    public BigDecimal calculateEffectiveAmount() {
        BigDecimal effective = amount;
        
        if (adjustmentFactor != null) {
            effective = effective.multiply(adjustmentFactor);
        }
        
        if (multiplier != null) {
            effective = effective.multiply(multiplier);
        }
        
        return effective;
    }
    
    /**
     * Checks if the cashflow is a payment (positive amount).
     */
    public boolean isPayment() {
        return amount != null && amount.compareTo(BigDecimal.ZERO) > 0;
    }
    
    /**
     * Checks if the cashflow is a receipt (negative amount).
     */
    public boolean isReceipt() {
        return amount != null && amount.compareTo(BigDecimal.ZERO) < 0;
    }
    
    /**
     * Checks if the cashflow is overdue.
     */
    public boolean isOverdue() {
        return paymentDate != null && 
               paymentDate.isBefore(LocalDate.now()) && 
               status != CashflowStatus.SETTLED &&
               status != CashflowStatus.CANCELLED;
    }
    
    /**
     * Checks if the cashflow is due today.
     */
    public boolean isDueToday() {
        return paymentDate != null && paymentDate.isEqual(LocalDate.now());
    }
    
    /**
     * Creates a copy with updated status.
     */
    public Cashflow withStatus(CashflowStatus newStatus, LocalDateTime timestamp) {
        return Cashflow.builder()
            .cashflowId(this.cashflowId)
            .positionId(this.positionId)
            .portfolioId(this.portfolioId)
            .instrument(this.instrument)
            .cashflowType(this.cashflowType)
            .amount(this.amount)
            .currency(this.currency)
            .paymentDate(this.paymentDate)
            .accrualStartDate(this.accrualStartDate)
            .accrualEndDate(this.accrualEndDate)
            .dayCountFraction(this.dayCountFraction)
            .notionalAmount(this.notionalAmount)
            .rate(this.rate)
            .spread(this.spread)
            .multiplier(this.multiplier)
            .adjustmentFactor(this.adjustmentFactor)
            .discountFactor(this.discountFactor)
            .presentValue(this.presentValue)
            .status(newStatus)
            .calculationMethod(this.calculationMethod)
            .businessDayConvention(this.businessDayConvention)
            .calendar(this.calendar)
            .resetDate(this.resetDate)
            .fixingDate(this.fixingDate)
            .settlementDate(this.settlementDate)
            .payerParty(this.payerParty)
            .receiverParty(this.receiverParty)
            .calculationDetails(this.calculationDetails)
            .riskFactors(this.riskFactors)
            .createdAt(this.createdAt)
            .updatedAt(timestamp)
            .version(this.version + 1)
            .build();
    }
    
    /**
     * Gets the effective currency for settlement.
     */
    public String getSettlementCurrency() {
        return currency != null ? currency : "USD";
    }
    
    /**
     * Creates a risk summary for the cashflow.
     */
    public Map<String, BigDecimal> createRiskSummary() {
        return Map.of(
            "notional_exposure", notionalAmount != null ? notionalAmount : BigDecimal.ZERO,
            "cashflow_amount", amount != null ? amount : BigDecimal.ZERO,
            "present_value", calculatePresentValue(),
            "days_to_payment", BigDecimal.valueOf(
                paymentDate != null ? 
                java.time.temporal.ChronoUnit.DAYS.between(LocalDate.now(), paymentDate) : 0
            )
        );
    }
}
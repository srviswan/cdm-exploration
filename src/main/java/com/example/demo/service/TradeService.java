package com.example.demo.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.regnosys.rosetta.common.serialisation.RosettaObjectMapper;
import com.rosetta.model.metafields.FieldWithMetaString;
import com.rosetta.model.metafields.MetaFields;
import cdm.base.math.NonNegativeQuantitySchedule;
import cdm.base.math.QuantityChangeDirectionEnum;
import cdm.base.math.UnitType;
import cdm.event.common.QuantityChangeInstruction;
import cdm.product.common.settlement.PriceQuantity;
import cdm.event.common.Trade;
import cdm.event.common.TradeState;
import cdm.product.template.TradableProduct;
import cdm.product.template.Product;
import cdm.product.template.EconomicTerms;
import cdm.product.qualification.functions.Qualify_EquitySwap_TotalReturnBasicPerformance_SingleName.Qualify_EquitySwap_TotalReturnBasicPerformance_SingleNameDefault;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.io.IOException;
import java.math.BigDecimal;

@Service
public class TradeService {
    private static final String CURRENCY_SCHEME = "someurl";
    
    private final RestTemplate restTemplate;
    
        public TradeService(RestTemplate restTemplate) {
            this.restTemplate = restTemplate;
        }
    
        public Object readTradeFromUrl(String url) {
            String jsonContent = restTemplate.getForObject(url, String.class);
            try {
                // Parse the Trade JSON from the root object
                TradeState tradeState = RosettaObjectMapper.getNewRosettaObjectMapper().readValue(jsonContent, TradeState.class);

                // Extract product from the trade
                Trade trade = tradeState.getTrade();
                EconomicTerms economicTerms = trade.getTradableProduct().getProduct().getContractualProduct().getEconomicTerms();
                
                // The qualification function expects EconomicTerms
                if (!(economicTerms instanceof EconomicTerms)) {
                    throw new RuntimeException("Trade product is not an instance of EconomicTerms");
                }
                
                boolean isQualified = new Qualify_EquitySwap_TotalReturnBasicPerformance_SingleNameDefault()
                    .evaluate((EconomicTerms) economicTerms);
                
                if (!isQualified) {
                    throw new RuntimeException("Trade does not qualify as a single name equity swap");
                }

                // Do an unwind of 70000 quantity from existing position
                QuantityChangeInstruction quantityChangeInstruction = QuantityChangeInstruction.builder()
                    .setDirection(QuantityChangeDirectionEnum.DECREASE)
                    .addChange(PriceQuantity.builder()
                        .addQuantityValue(NonNegativeQuantitySchedule.builder()
                            .setValue(BigDecimal.valueOf(70000))
                            .setUnit(UnitType.builder()
                                .setCurrency(FieldWithMetaString.builder()
                                    .setValue("USD")
                                    .setMeta(MetaFields.builder().setScheme(CURRENCY_SCHEME).build())
                                    .build())
                                .build())
                            .build())
                        .build())
                    .build();
                
                return quantityChangeInstruction;
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse trade JSON", e);
        }
    }
}

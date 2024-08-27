package xyz.sadiulhakim.project2.transaction;

import lombok.Setter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Setter
public class BalanceAdjusterProcessor implements ItemProcessor<BankTransaction, CurrencyAdjustment> {
    private StepExecution stepExecution;
    private double rate;

    public void safeExit(){
        stepExecution.setTerminateOnly();
    }

    @Override
    public CurrencyAdjustment process(BankTransaction item) throws Exception {
        CurrencyAdjustment currencyAdjustment = new CurrencyAdjustment();
        currencyAdjustment.id = item.getId();
        currencyAdjustment.adjustedAmount = item.getAmount()
                .multiply(BigDecimal.valueOf(rate))
                .setScale(2, RoundingMode.HALF_UP);
        return currencyAdjustment;
    }
}

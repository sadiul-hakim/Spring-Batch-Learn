package xyz.sadiulhakim.project2.transaction;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.util.Map;

public class MerchantMonthBalance {
    private final int month;
    private final String merchant;
    private final BigDecimal balance;

    public static final RowMapper<MerchantMonthBalance> ROW_MAPPER = (rs, rowNum) -> new MerchantMonthBalance(
            rs.getInt("month"),
            rs.getString("merchant"),
            rs.getBigDecimal("balance")
    );

    public static PagingQueryProvider getQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("sum(amount) as balance, merchant, month");
        queryProvider.setFromClause("bank_transaction");
        queryProvider.setGroupClause("month, merchant");
        queryProvider.setSortKeys(Map.of(
                "month", Order.ASCENDING,
                "merchant", Order.ASCENDING));
        return queryProvider;
    }

    public MerchantMonthBalance(int month, String merchant, BigDecimal balance) {
        this.month = month;
        this.merchant = merchant;
        this.balance = balance;
    }

    public int getMonth() {
        return month;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public String getMerchant() {
        return merchant;
    }
}

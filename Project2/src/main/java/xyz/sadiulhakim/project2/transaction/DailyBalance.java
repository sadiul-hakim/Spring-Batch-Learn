package xyz.sadiulhakim.project2.transaction;


import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.util.Map;

public class DailyBalance {
    private final int day;
    private final int month;
    private final BigDecimal balance;

    public DailyBalance(int day, int month, BigDecimal balance) {
        this.day = day;
        this.month = month;
        this.balance = balance;
    }

    // Row mapper to transform query results into Java object
    public static final RowMapper<DailyBalance> ROW_MAPPER = (rs, rowNum) -> new DailyBalance(
            rs.getInt("day"),
            rs.getInt("month"),
            rs.getBigDecimal("balance")
    );

    // Query provider to obtain daily balance aggregation from 'bank_transaction_yearly' table
    public static PagingQueryProvider getQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("sum(amount) as balance, day, month");
        queryProvider.setFromClause("bank_transaction");
        queryProvider.setGroupClause("day, month");
        queryProvider.setSortKeys(Map.of(
                "month", Order.ASCENDING,
                "day", Order.ASCENDING));
        return queryProvider;
    }

    public int getDay() {
        return day;
    }

    public int getMonth() {
        return month;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    @Override
    public String toString() {
        return "DailyBalance{" +
                "day=" + day +
                ", month=" + month +
                ", balance=" + balance +
                '}';
    }
}

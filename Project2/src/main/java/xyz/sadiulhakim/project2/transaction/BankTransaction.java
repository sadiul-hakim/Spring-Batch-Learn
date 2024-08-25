package xyz.sadiulhakim.project2.transaction;

import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;


public class BankTransaction {

    // Query and row mapper for obtaining bank transactions from the database
    public static final String SELECT_ALL_QUERY = "select * from bank_transaction";
    public static final RowMapper<BankTransaction> ROW_MAPPER = (rs, rowNum) -> new BankTransaction(
            rs.getLong("id"),
            rs.getInt("month"),
            rs.getInt("day"),
            rs.getInt("hour"),
            rs.getInt("minute"),
            rs.getBigDecimal("amount"),
            rs.getString("merchant"),
            rs.getBigDecimal("balance"),
            rs.getBoolean("adjusted")
    );

    private long id;
    private int month;
    private int day;
    private int hour;
    private int minute;
    private BigDecimal amount;
    private String merchant;
    private BigDecimal balance;
    private boolean adjusted;

    public BankTransaction(long id, int month, int day, int hour, int minute, BigDecimal amount, String merchant,
                           BigDecimal balance, boolean adjusted) {
        this.id = id;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.amount = amount;
        this.merchant = merchant;
        this.balance = balance;
        this.adjusted = adjusted;
    }

    public BankTransaction(long id, int month, int day, int hour, int minute, BigDecimal amount, String merchant) {
        this.id = id;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.amount = amount;
        this.merchant = merchant;
    }

    public long getId() {
        return id;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getMerchant() {
        return merchant;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public boolean isAdjusted() {
        return adjusted;
    }

    @Override
    public String toString() {
        return "BankTransaction{" +
                "id=" + id +
                ", month=" + month +
                ", day=" + day +
                ", hour=" + hour +
                ", minute=" + minute +
                ", amount=" + amount +
                ", merchant='" + merchant + '\'' +
                ", balance=" + balance +
                '}';
    }
}

package xyz.sadiulhakim.project2.transaction;

import java.math.BigDecimal;

public record BalanceUpdate(
        long id,
        BigDecimal balance
) {
}

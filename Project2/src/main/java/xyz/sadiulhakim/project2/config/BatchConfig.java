package xyz.sadiulhakim.project2.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import xyz.sadiulhakim.project2.transaction.*;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Configuration
public class BatchConfig {

    @Value("file:daily_balance.json")
    private WritableResource dailyBalanceJsonResource;

    @Value("file:monthly_balance.json")
    private WritableResource monthlyBalanceJsonResource;


    public static final String POSITIVE = "POSITIVE";
    public static final String NEGATIVE = "NEGATIVE";

    @Bean
    @Qualifier
    public Job bankTransactionAnalysisJob(JobRepository jobRepository,
                                          @Qualifier("fillBalanceStep") Step fillBalanceStep,
                                          @Qualifier("aggregateByMerchantMonthlyStep") Step aggregateByMerchantMonthlyStep,
                                          @Qualifier("aggregateByMerchantDailyStep") Step aggregateByMerchantDailyStep
    ) {
        return new JobBuilder("bankTransactionAnalysis-job", jobRepository)
                .start(fillBalanceStep)
                .on(NEGATIVE).to(aggregateByMerchantMonthlyStep)
                .from(fillBalanceStep).on(NEGATIVE).to(aggregateByMerchantDailyStep)
                .from(fillBalanceStep).on("*").end()
                .end()
                .build();
    }

    @Bean
    @Qualifier
    public Step fillBalanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                DataSource dataSource) {

        var processor = new FillBalanceProcessor();

        return new StepBuilder("fill-balance", jobRepository)
                .<BankTransaction, BalanceUpdate>chunk(10, transactionManager)
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                        .dataSource(dataSource)
                        .name("bankTransactionReader")
                        .sql(BankTransaction.SELECT_ALL_QUERY)
                        .rowMapper(BankTransaction.ROW_MAPPER)
                        .build())
                .processor(processor)
                .writer(new JdbcBatchItemWriterBuilder<BalanceUpdate>()
                        .dataSource(dataSource)
                        .itemPreparedStatementSetter(((item, ps) -> {
                            ps.setBigDecimal(1, item.balance());
                            ps.setLong(2, item.id());
                        }))
                        .sql("update bank_transaction set balance = ? where id = ?")
                        .build()
                )
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        processor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        double latestTransactionBalance = processor.getLatestTransactionBalance();
                        processor.setStepExecution(null);
                        return new ExitStatus(latestTransactionBalance >= 0 ? POSITIVE : NEGATIVE);
                    }
                })
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier
    public Step aggregateByMerchantMonthlyStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
                                               @Qualifier("merchantMonthAggregationReader") ItemReader<MerchantMonthBalance> merchantMonthAggregationReader) {
        return new StepBuilder("aggregateByMerchantMonthly-step", jobRepository)
                .<MerchantMonthBalance, MerchantMonthBalance>chunk(10, platformTransactionManager)
                .reader(merchantMonthAggregationReader)
                .writer(new JsonFileItemWriterBuilder<MerchantMonthBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(monthlyBalanceJsonResource)
                        .name("merchantMonthAggregationWriter")
                        .build()
                )
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier
    public Step aggregateByMerchantDailyStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager,
                                             @Qualifier("dailyBalanceAggregationReader") ItemReader<DailyBalance> dailyBalanceAggregationReader) {
        return new StepBuilder("aggregateByMerchantDaily-step", jobRepository)
                .<DailyBalance, DailyBalance>chunk(10, platformTransactionManager)
                .reader(dailyBalanceAggregationReader)
                .writer(new JsonFileItemWriterBuilder<DailyBalance>()
                        .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                        .resource(dailyBalanceJsonResource)
                        .name("merchantDailyAggregationWriter")
                        .build()
                )
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("merchantMonthAggregationReader")
    public ItemReader<MerchantMonthBalance> merchantMonthAggregationReader(DataSource sourceDataSource) {
        // Paging-style reader
        return new JdbcPagingItemReaderBuilder<MerchantMonthBalance>()
                .name("merchantMonthAggregationReader")
                .dataSource(sourceDataSource)
                .queryProvider(MerchantMonthBalance.getQueryProvider())
                .rowMapper(MerchantMonthBalance.ROW_MAPPER)
                // Querying the database in chinks of 5
                .pageSize(5)
                .build();
    }

    @Bean
    @Qualifier("dailyBalanceAggregationReader")
    public ItemReader<DailyBalance> dailyBalanceAggregationReader(DataSource sourceDataSource) {
        // Paging-style reader
        return new JdbcPagingItemReaderBuilder<DailyBalance>()
                .name("dailyBalanceAggregationReader")
                .dataSource(sourceDataSource)
                .queryProvider(DailyBalance.getQueryProvider())
                .rowMapper(DailyBalance.ROW_MAPPER)
                // Querying the database in chinks of 5
                .pageSize(5)
                .build();
    }


    // Balance Adjuster

    private static class CurrencyAdjustment{
        long id;
        BigDecimal adjustedAmount;
    }

    @Bean
    @Qualifier
    public Job balanceAdjusterJob(JobRepository jobRepository,
                                  @Qualifier("balanceAdjusterStep") Step balanceAdjusterStep){
        return new JobBuilder("balanceAdjuster-Job",jobRepository)
                .start(balanceAdjusterStep)
                .build();

    }

    @Bean
    @Qualifier
    public Step balanceAdjusterStep(JobRepository jobRepository,PlatformTransactionManager transactionManager,
                                    DataSource dataSource,
                                    @Value("${currency.adjustment.rate}") double rate,
                                    @Value("${currency.adjustment.disallowed.merchant}") String disallowedMerchant){
        return new StepBuilder("balanceAdjuster-Step",jobRepository)
                .<BankTransaction,CurrencyAdjustment>chunk(1,transactionManager)
                .reader(new JdbcCursorItemReaderBuilder<BankTransaction>()
                        .name("balanceAdjuster-reader")
                        .dataSource(dataSource)
                        .sql(BankTransaction.SELECT_ALL_QUERY+" where adjusted = false")
                        .rowMapper(BankTransaction.ROW_MAPPER)
                        .saveState(false)
                        .build()
                )
                .processor(item -> {
                    CurrencyAdjustment currencyAdjustment = new CurrencyAdjustment();
                    currencyAdjustment.id = item.getId();
                    currencyAdjustment.adjustedAmount = item.getAmount()
                            .multiply(BigDecimal.valueOf(rate))
                            .setScale(2, RoundingMode.HALF_UP);
                    return currencyAdjustment;
                })
                .writer(new JdbcBatchItemWriterBuilder<CurrencyAdjustment>()
                        .dataSource(dataSource)
                        .itemPreparedStatementSetter(((item, ps) -> {
                            ps.setBigDecimal(1, item.adjustedAmount);
                            ps.setBoolean(2, true); // Flag is set to true now
                            ps.setLong(3, item.id);
                        }))
                        .sql("update bank_transaction set amount = ?, adjusted = ? where id = ?")
                        .build()
                )
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

                        jdbcTemplate.update("alter table bank_transaction add column if not exists adjusted boolean default false");
                    }
                })
                .allowStartIfComplete(true)
                .build();

    }
}

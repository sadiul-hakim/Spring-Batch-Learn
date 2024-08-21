package xyz.sadiulhakim.project1.data;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class AppConfig {
    @Value("classpath:static/Data.txt")
    private Resource rawDailyInputResource;

    @Value("file:format.xml")
    private WritableResource aggreratedDailyOutXmlResource;

    @Value("file:HTE2NP-anomalies.csv")
    private WritableResource anomalyDataResource;

    @Bean
    public Job temperatureSensorJob(JobRepository jobRepository,
                                    @Qualifier("aggregateSensorStep") Step aggregateSensorStep,
                                    @Qualifier("reportAnomaliesStep") Step reportAnomaliesStep
    ) {
        return new JobBuilder("temperatureSensorJob", jobRepository)
                .start(aggregateSensorStep)
                .next(reportAnomaliesStep)
                .build();
    }

    @Bean
    @Qualifier
    public Step aggregateSensorStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("aggregate-step", jobRepository)
                .<DailySensorData, DailyAggregatedSensorData>chunk(1, transactionManager)
                .reader(new FlatFileItemReaderBuilder<DailySensorData>()
                        .name("dailySensorDataReader")
                        .resource(rawDailyInputResource)
                        .lineMapper(new SensorDataTextMapper())
                        .build())
                .processor(new RawToAggregateSensorDataProcessor())
                .writer(new StaxEventItemWriterBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataWritter")
                        .marshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggreratedDailyOutXmlResource)
                        .rootTagName("data")
                        .overwriteOutput(true)
                        .build()
                )
                .build();
    }

    @Bean
    @Qualifier
    public Step reportAnomaliesStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("reportAnomalies-step", jobRepository)
                .<DailyAggregatedSensorData, DataAnomaly>chunk(1, transactionManager)
                .reader(new StaxEventItemReaderBuilder<DailyAggregatedSensorData>()
                        .name("dailyAggregatedSensorDataReader")
                        .unmarshaller(DailyAggregatedSensorData.getMarshaller())
                        .resource(aggreratedDailyOutXmlResource)
                        .addFragmentRootElements(DailyAggregatedSensorData.ITEM_ROOT_ELEMENT_NAME)
                        .build()
                )
                .processor(new SensorDataAnomalyProcessor())
                .writer(new FlatFileItemWriterBuilder<DataAnomaly>()
                        .name("dataAnomalyWriter")
                        .resource(anomalyDataResource)
                        .delimited()
                        .delimiter(",")
                        .names(new String[]{"date", "type", "value"})
                        .build()
                )
                .build();
    }
}
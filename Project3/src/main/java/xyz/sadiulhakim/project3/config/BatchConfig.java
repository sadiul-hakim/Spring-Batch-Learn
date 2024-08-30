package xyz.sadiulhakim.project3.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.JvmCommandRunner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import xyz.sadiulhakim.project3.teamScore.*;

import java.io.Writer;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Configuration
public class BatchConfig {

    @Value("classpath:input/*.txt")
    private Resource[] inDivisionResources;

    @Value("file:calculated/avg.txt")
    private WritableResource outAvgResource;

    @Value("file:calculated/max.txt")
    private WritableResource maxPerformanceRatioOutResource;

    @Value("file:calculated/min.txt")
    private WritableResource minPerformanceRatioOutResource;

    @Value("file:calculated/")
    private WritableResource calculatedDirectoryResource;

    @Bean
    public Job teamPerformanceJob(
            JobRepository jobRepository,
            @Qualifier("threadPoolTaskExecutor") TaskExecutor threadPoolTaskExecutor,
            @Qualifier("averageTeamScoreStep") Step averageTeamScoreStep,
            @Qualifier("teamMaxPerformanceStep") Step teamMaxPerformanceStep,
            @Qualifier("teamMinPerformanceStep") Step teamMinPerformanceStep,
            @Qualifier("shellScriptStep") Step shellScriptStep,
            @Qualifier("successLoggerStep") Step successLoggerStep
    ) {
        var teamMaxPerformanceFlow = new FlowBuilder<SimpleFlow>("teamMaxPerformanceFlow")
                .start(teamMaxPerformanceStep)
                .build();
        var teamMinPerformanceFlow = new FlowBuilder<SimpleFlow>("teamMinPerformanceFlow")
                .start(teamMinPerformanceStep)
                .build();

        var performanceSplitFlow = new FlowBuilder<SimpleFlow>("performanceSplitFlow")
                .split(threadPoolTaskExecutor)
                .add(teamMaxPerformanceFlow, teamMinPerformanceFlow)
                .build();

        var averageTeamScoreFlow = new FlowBuilder<SimpleFlow>("averageTeamScoreFlow")
                .start(averageTeamScoreStep)
                .build();

        return new JobBuilder("teamPerformanceJob", jobRepository)
                .start(averageTeamScoreFlow)
                .next(performanceSplitFlow)
                .next(shellScriptStep)
                .next(successLoggerStep)
                .build()
                .build();
    }

    @Bean
    @Qualifier
    public TaskExecutor threadPoolTaskExecutor() {

        // One way
//        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
//        taskExecutor.setCorePoolSize(2);
//        return taskExecutor;

        // Rockstar way
        return new SimpleAsyncTaskExecutor(r -> Thread.ofVirtual().unstarted(r));
    }

    @Bean
    @Qualifier
    public Step averageTeamScoreStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                     @Qualifier("divisionItemReader") ItemReader<Team> divisionItemReader,
                                     @Qualifier("teamAverageProcessor") TeamAverageProcessor teamAverageProcessor,
                                     @Qualifier("jobStartLoggerListener") StepExecutionListener jobStartLoggerListener,
                                     @Qualifier("teamAverageContextPromotionListener") StepExecutionListener teamAverageContextPromotionListener) {
        return new StepBuilder("averageTeamScore-step", jobRepository)
                .<Team, AverageScoredTeam>chunk(1, transactionManager)
                .reader(divisionItemReader)
                .processor(teamAverageProcessor)
                .writer(new FlatFileItemWriterBuilder<AverageScoredTeam>()
                        .name("Team Average Scope Writer")
                        .resource(outAvgResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(item -> new Object[]{item.name(), item.averageScore()})
                        .build()
                )
                .listener(jobStartLoggerListener)
                .listener(teamAverageContextPromotionListener)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(stepExecution);
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        teamAverageProcessor.setStepExecution(null);
                        return StepExecutionListener.super.afterStep(stepExecution); // This line means, just continue
                    }
                })
                .faultTolerant()
                .skip(IndexOutOfBoundsException.class)
                .noSkip(NullPointerException.class)
                .skipLimit(40)
                .retry(RuntimeException.class)
                .noRetry(NullPointerException.class)
                .retryLimit(40)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier
    public TeamAverageProcessor teamAverageProcessor(@Value("#{jobParameters['scoreRank']}") int scoreRank) {
        return new TeamAverageProcessor(scoreRank);
    }

    @Bean
    @Qualifier
    public ItemReader<Team> divisionItemReader() {
        var flatFileItemReader = new FlatFileItemReaderBuilder<String>()
                .name("divisionLineReader")
                .lineMapper(((line, lineNumber) -> line))
                .build();

        var divisionFileReader = new DivisionFileReader(flatFileItemReader);

        return new MultiResourceItemReaderBuilder<Team>()
                .name("divisionTeamReader")
                .delegate(divisionFileReader)
                .resources(inDivisionResources)
                .build();

    }

    @Bean
    @Qualifier
    public ExecutionContextPromotionListener teamAverageContextPromotionListener() {
        var listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[]{
                TeamAverageProcessor.MAX_SCORE,
                TeamAverageProcessor.MAX_PLAYER,
                TeamAverageProcessor.MIN_SCORE,
                TeamAverageProcessor.MIN_PLAYER,
        }); // This listener is used to put above parameters from Step ExecutionContent to Job ExecutionContent
        return listener;
    }

    @Bean
    @StepScope
    @Qualifier
    public StepExecutionListener jobStartLoggerListener(@Value("#{jobParameters['uuid']}") String jobId) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                log.info("Job {} is started.", jobId);
            }
        };
    }

    /*------------Step Two----------------*/
    @Bean
    @Qualifier
    public Step teamMaxPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       @Qualifier("maxHeaderWriter") FlatFileHeaderCallback maxHeaderWriter,
                                       @Qualifier("maxRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor) {
        return new StepBuilder("teamMaxPerformance-step", jobRepository)
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                .reader(averageScoredTeamItemReader())
                .processor(maxRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMaxPerformance-writer")
                        .resource(maxPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(item -> new Object[]{item.name(), item.performance()})
                        .headerCallback(maxHeaderWriter)
                        .build()
                )
                .build();
    }

    @Bean
    @StepScope
    @Qualifier
    public ItemProcessor<AverageScoredTeam, TeamPerformance> maxRatioPerformanceProcessor(
            @Value("#{jobExecutionContext['max.score']}") double maxScore
    ) {
        return item -> process(item, maxScore);
    }

    @Bean
    @StepScope
    @Qualifier
    public FlatFileHeaderCallback maxHeaderWriter(@Value("#{jobExecutionContext['max.score']}") String maxScore,
                                                  @Value("#{jobExecutionContext['max.player']}") String maxPlayer) {
        return writer -> writeHeader(writer, maxPlayer, maxScore);
    }

    /*--------------------Step Three-----------------------------*/

    @Bean
    @Qualifier
    public Step teamMinPerformanceStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       @Qualifier("minHeaderWriter") FlatFileHeaderCallback minHeaderWriter,
                                       @Qualifier("minRatioPerformanceProcessor") ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor) {
        return new StepBuilder("teamMinPerformance-step", jobRepository)
                .<AverageScoredTeam, TeamPerformance>chunk(1, transactionManager)
                .reader(averageScoredTeamItemReader())
                .processor(minRatioPerformanceProcessor)
                .writer(new FlatFileItemWriterBuilder<TeamPerformance>()
                        .name("teamMinPerformance-writer")
                        .resource(minPerformanceRatioOutResource)
                        .delimited()
                        .delimiter(",")
                        .fieldExtractor(item -> new Object[]{item.name(), item.performance()})
                        .headerCallback(minHeaderWriter)
                        .build()
                )
                .build();
    }

    @Bean
    @StepScope
    @Qualifier
    public ItemProcessor<AverageScoredTeam, TeamPerformance> minRatioPerformanceProcessor(
            @Value("#{jobExecutionContext['min.score']}") double minScore
    ) {
        return item -> process(item, minScore);
    }

    @Bean
    @StepScope
    @Qualifier
    public FlatFileHeaderCallback minHeaderWriter(@Value("#{jobExecutionContext['min.score']}") String minScore,
                                                  @Value("#{jobExecutionContext['min.player']}") String minPlayer) {
        return writer -> writeHeader(writer, minPlayer, minScore);
    }

    /*------------------Tasklet--------------------*/

    @Bean
    public Step shellScriptStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                @Qualifier("shellScriptTasklet") Tasklet shellScriptTasklet) {
        return new StepBuilder("shellScript-step", jobRepository)
                .tasklet(shellScriptTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step successLoggerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                  @Qualifier("successLoggerTasklet") Tasklet successLoggerTasklet) {
        return new StepBuilder("successLogger-step", jobRepository)
                .tasklet(successLoggerTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier
    public Tasklet shellScriptTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return ((contribution, chunkContext) -> {
            var runner = new JvmCommandRunner();
            runner.exec(new String[]{"bash", "-l", "-c", "touch" + uuid + ".resulted"},
                    new String[]{}, calculatedDirectoryResource.getFile());

            return RepeatStatus.FINISHED;
        });
    }

    @Bean
    @StepScope
    @Qualifier
    public Tasklet successLoggerTasklet(@Value("#{jobParameters['uuid']}") String uuid) {
        return (contribution, chunkContext) -> {
            log.info("Job with uuid = {} is finished", uuid);
            return RepeatStatus.FINISHED;
        };
    }

    /*------------------End------------------------*/

    private static TeamPerformance process(AverageScoredTeam team, double baselineScore) {
        BigDecimal performance = BigDecimal.valueOf(team.averageScore())
                .multiply(new BigDecimal(100))
                .divide(BigDecimal.valueOf(baselineScore), 2, RoundingMode.HALF_UP);
        return new TeamPerformance(team.name(), performance + "%");
    }

    public ItemReader<AverageScoredTeam> averageScoredTeamItemReader() {
        return new FlatFileItemReaderBuilder<AverageScoredTeam>()
                .name("averageScoredTeamItemReader")
                .resource(outAvgResource)
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .fieldSetMapper(fieldSet -> new AverageScoredTeam(fieldSet.readString(0), fieldSet.readDouble(1)))
                .build();
    }

    private static void writeHeader(Writer writer, String name, String score) {
        try {
            writer.write("====================================================================\n");
            writer.write("Team performances below are calculated against " + score + " which was scored by " + name + "\n");
            writer.write("====================================================================\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

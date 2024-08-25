package xyz.sadiulhakim.project2.transaction;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {
    private final JobLauncher jobLauncher;
    private final Job bankTransactionAnalysisJob;
    private final Job balanceAdjusterJob;

    public JobController(JobLauncher jobLauncher, @Qualifier("bankTransactionAnalysisJob") Job bankTransactionAnalysisJob,
                         @Qualifier("balanceAdjusterJob") Job balanceAdjusterJob) {
        this.jobLauncher = jobLauncher;
        this.bankTransactionAnalysisJob = bankTransactionAnalysisJob;
        this.balanceAdjusterJob = balanceAdjusterJob;
    }

    @GetMapping("/analysis")
    public String analysis() {
        var parameter = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            jobLauncher.run(bankTransactionAnalysisJob, parameter);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }

        return "Done";
    }

    @GetMapping("/adjust")
    public String adjust() {
        var parameter = new JobParametersBuilder()
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            jobLauncher.run(balanceAdjusterJob, parameter);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }

        return "Done";
    }
}

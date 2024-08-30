package xyz.sadiulhakim.project3.teamScore;

import lombok.Setter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

public class TeamAverageProcessor implements ItemProcessor<Team, AverageScoredTeam> {
    public static String MAX_PLAYER = "max.player";
    public static String MIN_SCORE = "min.score";
    public static String MIN_PLAYER = "min.player";
    public static String MAX_SCORE = "max.score";

    private final int scoreRank;
    @Setter
    private StepExecution stepExecution;

    public TeamAverageProcessor(int scoreRank) {
        this.scoreRank = scoreRank;
    }

    @Override
    public AverageScoredTeam process(Team team) throws Exception {
        if (stepExecution == null) {
            throw new RuntimeException("Team Average can not be processed without Step Execution set");
        }

        ExecutionContext executionContext = stepExecution.getExecutionContext();

        double maxScore = executionContext.containsKey(MAX_SCORE) ? executionContext.getDouble(MAX_SCORE) : 0.0;
        double minScore = executionContext.containsKey(MIN_SCORE) ? executionContext.getDouble(MIN_SCORE) : 0.0;

        double sum = 0;
        double count = 0;
        for (Team.Player player : team.getScoredPlayers()) {
            double score = player.getScores().get(scoreRank);
            if (score > maxScore) {
                executionContext.putDouble(MAX_SCORE, score);
                executionContext.putString(MAX_PLAYER, player.getName());
                maxScore = score;
            }

            if (score < minScore) {
                executionContext.putDouble(MIN_SCORE, score);
                executionContext.putString(MIN_PLAYER, player.getName());
                minScore = score;
            }

            sum += score;
            count++;
        }

        return new AverageScoredTeam(team.getName(), sum / count);
    }
}

package xyz.sadiulhakim.project3.teamScore;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class DivisionFileReader implements ResourceAwareItemReaderItemStream<Team> {
    private final FlatFileItemReader<String> delegatedReader;

    public DivisionFileReader(FlatFileItemReader<String> delegatedReader) {
        this.delegatedReader = delegatedReader;
    }

    @Override
    public Team read() throws Exception {
        Optional<Team> maybeTeam = Optional.empty();
        String line;
        while ((line = delegatedReader.read()) != null) {
            if (line.isEmpty()) {
                return maybeTeam.orElse(null);
            } else if (!line.contains(":")) {
                maybeTeam = Optional.of(new Team(line));
            } else {
                String[] split = line.split(":");
                String playerName = split[0];
                String[] scoreArr = split[1].split(",");
                List<Double> scores = Arrays.stream(scoreArr).map(Double::parseDouble).toList();
                maybeTeam.ifPresent(team -> team.getScoredPlayers().add(new Team.Player(playerName, scores)));
            }
        }

        maybeTeam.ifPresent(System.out::println);

        return maybeTeam.orElse(null);
    }

    @Override
    public void setResource(Resource resource) {
        delegatedReader.setResource(resource);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        delegatedReader.open(executionContext);
    }


    @Override
    public void close() throws ItemStreamException {
        delegatedReader.close();
    }
}

package xyz.sadiulhakim.project3.teamScore;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
public class Team {
    private String name;
    private List<Player> scoredPlayers = new ArrayList<>();

    public Team(String name) {
        this.name = name;
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class Player {
        private String name;
        private List<Double> scores = new ArrayList<>();

        public Player(String name) {
            this.name = name;
        }
    }


}

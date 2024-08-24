package xyz.sadiulhakim.project2;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import xyz.sadiulhakim.project2.transaction.DailyBalance;

import java.util.List;

@SpringBootApplication
@RequiredArgsConstructor
public class Project2Application implements CommandLineRunner {

//    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

//    public Project2Application(@Qualifier("sourceDataSource") DataSource dataSource) {
//        this.dataSource = dataSource;
//    }

    public static void main(String[] args) {
        SpringApplication.run(Project2Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

//        var jdbcTemplate = new JdbcTemplate(dataSource);

        String query = "select * from bank_transaction limit 10";
        List<DailyBalance> query1 = jdbcTemplate.query(DailyBalance.getQueryProvider().generateFirstPageQuery(10), DailyBalance.ROW_MAPPER);

        System.out.println(query1);
    }
}

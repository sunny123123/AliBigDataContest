
mvn clean assembly:assembly
jstorm jar ./target/AliBigDataContest-0.0.1-SNAPSHOT.jar com.alibaba.middleware.race.jstorm.RaceTopology

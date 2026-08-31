# Flight Analyzer — Spark Scala

## Запуск

Локально:
```
spark-submit --class com.example.FlightAnalyzer --deploy-mode client --master local[*] --verbose --supervise flight-analyzer.jar flight-analyzer.jar airlines.csv airports.csv flights.csv output desc
```

На кластере:
```
spark-submit --class com.example.FlightAnalyzer --deploy-mode client --master <cluster-master> --verbose --supervise flight-analyzer.jar flight-analyzer.jar airlines.csv airports.csv flights.csv output desc
```

Аргументы: `airlines.csv airports.csv flights.csv output [asc|desc]`.
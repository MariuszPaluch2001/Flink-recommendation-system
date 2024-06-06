# REDIS
1. `sudo snap install redis`
2. `sudo apt install redis-tools`
# Kompilacja projektu
1. Aby skompilować wybrany moduł projektu (RealTimeRecommendations, Recommendations czy ProductReviewAggregation) należy ustawić odpowieni MainClass w pliku pom.xml, następnie uruchomić Mavena z podanym opcjami: `mvn clean install`
# Uruchomienie projektu
1. Sprawdzić działanie Kafki
2. Włączyś klaster Flinka (`start-cluster.sh`)
3. Przejść do folderu Redis i uruchomić skrypt wsadowy
4. Skompilować moduł Recommendations i uruchomić go przy pomocy Flinka (`flink run ....`)
5. Skompilować moduł ProductReviewAggregation i uruchomić go przy pomocy Flinka (`flink run ....`)
6. Przejść do folderu Kafka i uruchomić skrypt generujący
7. Skompilować moduł RealTimeRecommendations i uruchomić go przy pomocy Flinka (`flink run ....`)
# NODE.JS
1. `sudo apt-get install nodejs`
2. `sudo apt-get install npm`
3. `cd visualization`
4. `cd service`
5. `npm install`
6. `node service.js`
7. `cd ../web`
8. Otwarcie w przegladarce pliku index.html

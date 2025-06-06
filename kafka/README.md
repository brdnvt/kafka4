Система обробки даних Starbucks з використанням Apache Kafka

Даний проект представляє реалізацію системи потокової обробки даних на основі Apache Kafka для аналізу інформації про напої Starbucks. Система виконує комплексний аналіз характеристик напоїв, включаючи їх калорійність та склад, з подальшою класифікацією та агрегацією результатів.

Архітектура системи

Архітектура системи базується на використанні декількох взаємопов'язаних компонентів. PostgreSQL використовується як система зберігання вихідних даних. Apache Kafka забезпечує потокову обробку даних. Zookeeper відповідає за координацію компонентів Kafka. Веб-інтерфейс Kafka UI надає можливості моніторингу та візуалізації потоків даних.

Система реалізує два основних потоки даних. Перший потік містить базову інформацію про напої: назва, розмір, тип молока, наявність вершків та калорійність. Другий потік включає дані про поживну цінність: вміст жирів, холестерину, натрію, вуглеводів, цукру та кофеїну.

Функціональність

Система забезпечує розділення потоку напоїв на основі калорійності. Напої з калорійністю 200 ккал і вище класифікуються як висококалорійні, решта - як низькокалорійні. Операція join об'єднує інформацію про напої з даними про їх поживну цінність на основі назви продукту. Вікно для операції join встановлено на 5 хвилин, що забезпечує коректну обробку даних, які надходять з різною затримкою.

Технічні вимоги

Для функціонування системи потрібні: Java версії 8 або вище, Apache Maven для збірки проекту, Docker та Docker Compose для розгортання інфраструктури. Система використовує порти 5430 для PostgreSQL, 2181 для Zookeeper, 9092 для Kafka та 8080 для Kafka UI.

Процес розгортання

Розгортання системи починається з запуску контейнерів Docker через docker-compose. Після цього виконується компіляція проекту за допомогою Maven. Дані завантажуються з CSV-файлу до PostgreSQL, звідки передаються до Kafka. Kafka Streams виконує обробку даних, результати якої можна спостерігати через Kafka UI або спеціальний консольний споживач.

Моніторинг та аналіз

Система надає можливість аналізу даних через SQL-запити до PostgreSQL та через веб-інтерфейс Kafka UI. В базі даних можна отримати статистику щодо загальної кількості напоїв, кількості висококалорійних напоїв та їх розподілу за типами молока. Kafka UI відображає стан тем, кількість повідомлень та метрики продуктивності системи.

Результати обробки

В результаті роботи системи створюються теми Kafka, що містять оброблені дані: drinks-info та nutrition-info з вихідними даними, high-calorie-drinks та low-calorie-drinks з результатами розділення потоку, complete-drinks-info з об'єднаними даними про напої. Кожна тема має один розділ та фактор реплікації 1, що відповідає вимогам тестового середовища.

Проект виконує наступні кроки:
1. Завантажує дані з CSV-файлу до бази даних PostgreSQL.
2. Завантажує дані з PostgreSQL до теми Kafka (`coffee-products`).
3. Обробляє потік даних за допомогою Kafka Streams:
   - Фільтрує записи про напої, у яких більше 200 ккал.
   - Розділяє записи на три гілки за типом молока: без молока, з кокосовим молоком, інше.
   - Зберігає відфільтровані та розділені результати в окремих темах Kafka.
4. Споживає дані з результуючих тем для демонстрації оброблених даних.

Вимоги

Для запуску проекту вам потрібні:
- Java 8 або вище
- Apache Maven
- Docker та Docker Compose

Запуск інфраструктури Docker

Переконайтеся, що у вас встановлені Docker та Docker Compose.
У кореневій директорії проекту виконайте команду:


docker-compose up -d


Ця команда запустить наступні контейнери:
- PostgreSQL (порт 5430): база даних для зберігання початкових даних.
- Zookeeper (порт 2181): необхідний для роботи Kafka.
- Kafka (порт 9092): брокер повідомлень Kafka.
- Kafka UI (порт 8080): веб-інтерфейс для перегляду тем та повідомлень Kafka.

Перевірити стан запущених контейнерів можна командою:


docker ps


Компіляція проекту

Скомпілюйте проект за допомогою Maven:


cd kafka
mvn clean package


Ця команда збере проект у виконуваний JAR-файл з усіма залежностями (`target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar`).

Створення Kafka тем

Теми (`coffee-products`, `no_milk_drinks`, `coconut_milk_drinks`, `other_milk_drinks`) будуть створені автоматично при першому запуску відповідних компонентів Kafka.

Завантаження даних

1. Завантаження даних з CSV до PostgreSQL:
   Виконайте наступну команду з директорії

   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.CsvToPostgresLoader

   Це завантажить дані з файлу `starbucks.csv` (переконайтеся, що він знаходиться в директорії `kafka`) до бази даних PostgreSQL.

2. Завантаження даних з PostgreSQL до Kafka:
   Відкрийте новий термінал та запустіть Producer, який зчитає дані з PostgreSQL та надішле їх до теми `coffee-products`:


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaProducerFromDB

Запуск додатків Kafka

Відкрийте окремі термінали для кожного з наступних компонентів і запустіть їх :

1. Запуск Kafka Streams додатку:
   Цей додаток обробляє дані з теми `coffee-products`.


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaStreamsApp
   

2. Запуск Consumer для перегляду результатів:
   Цей додаток споживає дані з тем результатів (`no_milk_drinks`, `coconut_milk_drinks`, `other_milk_drinks`) і виводить їх у консоль.


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaStreamResultsConsumer
   

Перевірка результатів

Ви можете переглянути повідомлення у темах Kafka за допомогою Kafka UI. Відкрийте у браузері:


http://localhost:8080


Також можна перевірити дані безпосередньо в базі даних PostgreSQL за допомогою Docker:

- Перевірка загальної кількості напоїв у таблиці `coffee_products`:

  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT COUNT(*) FROM coffee_products;"
  

- Перевірка загальної кількості напоїв з калоріями > 200 в базі:
  
  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT COUNT(*) FROM coffee_products WHERE calories > 200;"
  

- Розподіл за категоріями молока (для напоїв > 200 ккал):
  
  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT CASE WHEN milk = 0 THEN 'Без молока' WHEN milk = 5 THEN 'Кокосове молоко' ELSE 'Інші типи молока' END AS категорія, COUNT(*) FROM coffee_products WHERE calories > 200 GROUP BY категорія ORDER BY категорія;"
  
  (Примітка: Цей запит дасть розподіл у БД, який має відповідати кількості повідомлень у вихідних темах Kafka після обробки Kafka Streams).

Очищення бази даних PostgreSQL

Ви можете очистити таблицю `coffee_products` в базі даних PostgreSQL за допомогою наступної команди (з директорії `kafka`):


java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.DatabaseCleaner


Зупинка проекту

Для зупинки всіх запущених Docker контейнерів виконайте команду в кореневій директорії проекту (`kafka3`):


docker-compose down

Тестові дані

Система підтримує два джерела тестових даних:

1. Статичний набір даних з CSV-файлу `starbucks.csv`. Файл містить інформацію про 1147 напоїв Starbucks з детальними характеристиками: назва продукту, розмір порції, тип молока, наявність вершків, калорійність, вміст жирів, холестерину, натрію, вуглеводів, цукру та кофеїну. Ці дані завантажуються до PostgreSQL за допомогою `CsvToPostgresLoader` та далі передаються до теми Kafka `coffee-products`.

2. Динамічно генеровані дані через `KafkaProducerApp`. Цей компонент створює випадкові записи про напої з наступними параметрами:
   - Product1-Product10: випадково згенеровані назви продуктів
   - Розмір: short або tall
   - Тип молока: 0 (без молока, 20% випадків), 5 (кокосове молоко, 30% випадків), 1-4 (інші типи молока, 50% випадків)
   - Калорійність: 0-300 ккал
   - Додаткові характеристики поживної цінності генеруються у реалістичних діапазонах

Генератор створює два окремих потоки даних: основну інформацію про напої (тема `drinks-info`) та дані про поживну цінність (тема `nutrition-info`). Потоки пов'язані між собою через назву продукту, що дозволяє демонструвати операцію join у Kafka Streams).



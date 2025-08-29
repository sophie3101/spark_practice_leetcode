This repository contains my solutions for LeetCode SQL challenges. This is a good way to practice using Apache Spark (PySpark) and Spark SQL

## Easy Level

| ID | description                     | Solution               |
| :------: | :------------------------------- | :-------------------- |
|    1280    | [question1280](https://leetcode.com/problems/students-and-examinations/description/) | [solution](easy_levels/1280_students_n_examinations.py) |

## Medium Level

| ID | description                     | Solution               |
| :------: | :------------------------------- | :-------------------- |
| 180| [question180](https://leetcode.com/problems/consecutive-numbers/description/) | [solution](medium_levels/180_consecutive_numbers.py) |
| 184| [question184](https://leetcode.com/problems/department-highest-salary/description/) | [solution](medium_levels/184_department_highest_salary.py) |
| 550| [question550](https://leetcode.com/problems/students-and-examinations/description/) | [solution](medium_levels/550_gam_play_analysis.py) |
| 570| [question570](https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/) | [solution](medium_levels/570_managers_w_at_least_5_reports.py) |
| 585| [question585](https://leetcode.com/problems/investments-in-2016/description/) | [solution](medium_levels/585_investments_2016.py) |
| 602| [question602](https://leetcode.com/problems/friend-requests-ii-who-has-the-most-friends/description/) | [solution](medium_levels/602_friends_requests.py) |
| 626| [question626](https://leetcode.com/problems/exchange-seats/description/) | [solution](medium_levels/626_exchange_seats.py) |
| 1045| [question1045](https://leetcode.com/problems/customers-who-bought-all-products/description/) | [solution](medium_levels/1045_customer_bought_all_products) |
| 1070| [question1070](https://leetcode.com/problems/product-sales-analysis-iii/description/) | [solution](medium_levels/1070_product_sale_analysis.py) |
| 1164| [question1164](https://leetcode.com/problems/product-price-at-a-given-date/description/) | [solution](medium_levels/1164_product_price_at_given_date.py) |
| 1174| [question1174](https://leetcode.com/problems/immediate-food-delivery-ii/description/) | [solution](medium_levels/1174_immediate_food_delivery.py) |
| 1193| [question1193](https://leetcode.com/problems/monthly-transactions-i/description/) | [solution](medium_levels/1193_monthly_transations.py) |
| 1204| [question1204](https://leetcode.com/problems/last-person-to-fit-in-the-bus/description/) | [solution](medium_levels/1204_last_person_on_bus.py) |
| 1934| [question1934](https://leetcode.com/problems/confirmation-rate/description/) | [solution](medium_levels/1934_confirmation_rate.py) |

## How to run PySpark script

1. using spark-submit locally: 

    spark-submit your_script.py

    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 4 \
      --executor-memory 2G \
      your_script.py


2. run interactively via pyspark shell
  pyspark
  Then a pyspark shell pop up

3. python spark_script_name.py

  This works only if the environment variables (SPARK_HOME, PYSPARK_PYTHON, etc.) are properly set.

4. Using docker

  ``` 
  docker run -it --rm \
      -v /path/to/your/project:/app \
      -p 4040:4040 \
      apache/spark:latest /bin/bash
  ```
  
Inside the container environment:

apt-get update && apt-get install -y python3 python3-pip
pip3 install pyspark
spark-submit --master local[*] /app/spark_script.py

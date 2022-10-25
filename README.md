# Importando Bibliotecas


```python
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

# Iniciando sessão Spark


```python
spark = (
        SparkSession.builder
        .master('local')
        .appName('SparkSession_02')
        .getOrCreate()
)
```

# Importando DataSet

   1. Importar Dataset, para coleta dos Dados.


```python
df = spark.read.csv('vinhos+no+mundo.csv', header=True, inferSchema=True)
```


```python
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
|_c0| country|         description|         designation|points|price|         province|           region_1|         region_2|       taster_name|taster_twitter_handle|               title|           variety|             winery|
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
|  0|   Italy|Aromas include tr...|        Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|         @kerinokeefe|Nicosia 2013 Vulk...|       White Blend|            Nicosia|
|  1|Portugal|This is ripe and ...|            Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|           @vossroger|Quinta dos Avidag...|    Portuguese Red|Quinta dos Avidagos|
|  2|      US|Tart and snappy, ...|                null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Rainstorm 2013 Pi...|        Pinot Gris|          Rainstorm|
|  3|      US|Pineapple rind, l...|Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|                 null|St. Julian 2013 R...|          Riesling|         St. Julian|
|  4|      US|Much like the reg...|Vintner's Reserve...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Sweet Cheeks 2012...|        Pinot Noir|       Sweet Cheeks|
|  5|   Spain|Blackberry and ra...|        Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|          @wineschach|Tandem 2011 Ars I...|Tempranillo-Merlot|             Tandem|
|  6|   Italy|Here's a bright, ...|             Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|         @kerinokeefe|Terre di Giurfo 2...|          Frappato|    Terre di Giurfo|
|  7|  France|This dry and rest...|                null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Trimbach 2012 Gew...|    Gewürztraminer|           Trimbach|
|  8| Germany|Savory dried thym...|               Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|                 null|Heinz Eifel 2013 ...|    Gewürztraminer|        Heinz Eifel|
|  9|  France|This has great de...|         Les Natures|    87| 27.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Jean-Baptiste Ada...|        Pinot Gris| Jean-Baptiste Adam|
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
```

# Renomeando Colunas 

   1. Renomear as colunas para deixar o dataset mais usual.
  


```python
df = df.withColumnRenamed('country', 'pais')\
    .withColumnRenamed('description', 'descricao')\
    .withColumnRenamed('designation', 'designacao')\
    .withColumnRenamed('points','pontos')\
    .withColumnRenamed('price', 'preco')\
    .withColumnRenamed('province','provincia')\
    .withColumnRenamed('region_1','regiao1')\
    .withColumnRenamed('region_2','regiao2')\
    .withColumnRenamed('taster_name','somelier')\
    .withColumnRenamed('taster_twitter_handle', 'twitter_somelier')\
    .withColumnRenamed('title','titulo')\
    .withColumnRenamed('variety','variacao')\
    .withColumnRenamed('winery','vinicola')\
    .withColumnRenamed('_c0', 'ID')
```


```python
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+----------------+--------------------+------------------+-------------------+
| ID|    pais|           descricao|          designacao|pontos|preco|        provincia|            regiao1|          regiao2|          somelier|twitter_somelier|              titulo|          variacao|           vinicola|
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+----------------+--------------------+------------------+-------------------+
|  0|   Italy|Aromas include tr...|        Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|    @kerinokeefe|Nicosia 2013 Vulk...|       White Blend|            Nicosia|
|  1|Portugal|This is ripe and ...|            Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|      @vossroger|Quinta dos Avidag...|    Portuguese Red|Quinta dos Avidagos|
|  2|      US|Tart and snappy, ...|                null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|     @paulgwine |Rainstorm 2013 Pi...|        Pinot Gris|          Rainstorm|
|  3|      US|Pineapple rind, l...|Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|            null|St. Julian 2013 R...|          Riesling|         St. Julian|
|  4|      US|Much like the reg...|Vintner's Reserve...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|     @paulgwine |Sweet Cheeks 2012...|        Pinot Noir|       Sweet Cheeks|
|  5|   Spain|Blackberry and ra...|        Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|     @wineschach|Tandem 2011 Ars I...|Tempranillo-Merlot|             Tandem|
|  6|   Italy|Here's a bright, ...|             Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|    @kerinokeefe|Terre di Giurfo 2...|          Frappato|    Terre di Giurfo|
|  7|  France|This dry and rest...|                null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|      @vossroger|Trimbach 2012 Gew...|    Gewürztraminer|           Trimbach|
|  8| Germany|Savory dried thym...|               Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|            null|Heinz Eifel 2013 ...|    Gewürztraminer|        Heinz Eifel|
|  9|  France|This has great de...|         Les Natures|    87| 27.0|           Alsace|             Alsace|             null|        Roger Voss|      @vossroger|Jean-Baptiste Ada...|        Pinot Gris| Jean-Baptiste Adam|
+---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+----------------+--------------------+------------------+-------------------+
```

# Excluindo Colunas

   1. Excluir colunas que não vão ser utilizadas na análise.


```python
df = df.drop('descricao', 'twitter_somelier','regiao2')
```

# Alterando o tipo dos dados

   1. Alterando o tipo dos dados das colunas: preço, pontos, de string para float e a coluna ID de string para Integer.


```python
df.withColumn('preco', col('preco').cast(FloatType()))\
    .withColumn('pontos',col('pontos').cast(FloatType()))\
    .withColumn('ID',col('ID').cast(IntegerType())).printSchema()
```

    root
     |-- ID: integer (nullable = true)
     |-- pais: string (nullable = true)
     |-- designacao: string (nullable = true)
     |-- pontos: float (nullable = true)
     |-- preco: float (nullable = true)
     |-- provincia: string (nullable = true)
     |-- regiao1: string (nullable = true)
     |-- somelier: string (nullable = true)
     |-- titulo: string (nullable = true)
     |-- variacao: string (nullable = true)
     |-- vinicola: string (nullable = true)
    
    

# Select 

   1. Criando select, filtrando apenas Itália e Brasil.
   2. Criando select, filtrando países: Portugal, Brasil e Itália. E que a pontuação dos vinhos sejam maiores que 85 e o preço maior que 30.


```python
df_select1 = df.select('ID','pais','provincia','titulo','pontos','preco')\
            .filter((col('pais') == "Italy") | (col('pais') == 'Brazil'))
```


```python
+---+-----+-----------------+--------------------+------+-----+
| ID| pais|        provincia|              titulo|pontos|preco|
+---+-----+-----------------+--------------------+------+-----+
|  0|Italy|Sicily & Sardinia|Nicosia 2013 Vulk...|    87| null|
|  6|Italy|Sicily & Sardinia|Terre di Giurfo 2...|    87| 16.0|
| 13|Italy|Sicily & Sardinia|Masseria Settepor...|    87| null|
| 22|Italy|Sicily & Sardinia|Baglio di Pianett...|    87| 19.0|
| 24|Italy|Sicily & Sardinia|Canicattì 2009 Ay...|    87| 35.0|
| 26|Italy|Sicily & Sardinia|Stemmari 2013 Dal...|    87| 13.0|
| 27|Italy|Sicily & Sardinia|Stemmari 2013 Ner...|    87| 10.0|
| 28|Italy|Sicily & Sardinia|Terre di Giurfo 2...|    87| 17.0|
| 31|Italy|Sicily & Sardinia|Duca di Salaparut...|    86| null|
| 32|Italy|Sicily & Sardinia|Duca di Salaparut...|    86| null|
+---+-----+-----------------+--------------------+------+-----+
```


```python
df_select2 = df.select('ID', 'pais', 'provincia', 'titulo', 'pontos', 'preco')\
            .filter((col('pais') == 'Portugal') | (col('pais') == 'Brazil') | (col('pais') == 'Italy'))\
            .filter((col('pontos') >= 85))\
            .filter((col('preco') > 30))
```


```python
+---+-----+-----------------+--------------------+------+-----+
| ID| pais|        provincia|              titulo|pontos|preco|
+---+-----+-----------------+--------------------+------+-----+
| 24|Italy|Sicily & Sardinia|Canicattì 2009 Ay...|    87| 35.0|
| 72|Italy|   Southern Italy|Grifalco 2013 Dag...|    86| 32.0|
|118|Italy|          Tuscany|Tenuta Forconi 20...|    87| 80.0|
|120|Italy|         Piedmont|Ceretto 2003 Bric...|    92| 70.0|
|130|Italy|         Piedmont|Ceretto 2003 Bric...|    91| 70.0|
|133|Italy|         Piedmont|Poderi Luigi Eina...|    91| 68.0|
|135|Italy|         Piedmont|Giacomo Ascheri 2...|    91| 60.0|
|141|Italy|         Piedmont|Giacomo Ascheri 2...|    90| 45.0|
|158|Italy|          Tuscany|Castello di Gabbi...|    91| 38.0|
|187|Italy|Sicily & Sardinia|Abbazia Santa Ana...|    88| 48.0|
+---+-----+-----------------+--------------------+------+-----+
```

# Manipulando novo arquivo

   1. Iniciar nova sessão spark, para manipular um novo arquivo.
   2. Importar o novo dataset.


```python
spark = (
        SparkSession.builder
        .master('local')
        .appName('SparkSession_03')
        .getOrCreate()
)
```


```python
df_food = spark.read.csv('food_coded.csv', header=True, inferSchema=True)
```


```python
+---------------+------+--------------------+----------------+------------+--------------+------+--------------------+--------------------+---------------------------+----+----------------------------+-------+--------------------+------------------+-----+--------------------+--------------------+---------------------+----------+----------+-----------+--------+----------------+-----------------+--------------------+-----------------+--------+--------------------+-----+---------+-----------+----------+---------------+--------------------+--------------------+----------------+------+-----------+------------+--------------+--------------+--------------------+----------------+--------------------+-----------------+-------------+------------+------------+------------+----------------------+----+------+---------+-----------------+---------------+-----------+-----------+--------+---------------+--------------------+
|            GPA|Gender|           breakfast|calories_chicken|calories_day|calories_scone|coffee|        comfort_food|comfort_food_reasons|comfort_food_reasons_coded9|cook|comfort_food_reasons_coded11|cuisine|        diet_current|diet_current_coded|drink|      eating_changes|eating_changes_coded|eating_changes_coded1|eating_out|employment|ethnic_food|exercise|father_education|father_profession|         fav_cuisine|fav_cuisine_coded|fav_food|      food_childhood|fries|fruit_day|grade_level|greek_food|healthy_feeling|        healthy_meal|          ideal_diet|ideal_diet_coded|income|indian_food|italian_food|life_rewarding|marital_status| meals_dinner_friend|mother_education|   mother_profession|nutritional_check|on_off_campus|parents_cook|pay_meal_out|persian_food|self_perception_weight|soup|sports|thai_food|tortilla_calories|turkey_calories|type_sports|veggies_day|vitamins|waffle_calories|              weight|
+---------------+------+--------------------+----------------+------------+--------------+------+--------------------+--------------------+---------------------------+----+----------------------------+-------+--------------------+------------------+-----+--------------------+--------------------+---------------------+----------+----------+-----------+--------+----------------+-----------------+--------------------+-----------------+--------+--------------------+-----+---------+-----------+----------+---------------+--------------------+--------------------+----------------+------+-----------+------------+--------------+--------------+--------------------+----------------+--------------------+-----------------+-------------+------------+------------+------------+----------------------+----+------+---------+-----------------+---------------+-----------+-----------+--------+---------------+--------------------+
|            2.4|     2|                   1|             430|         nan|           315|     1|                none|we dont have comf...|                          9|   2|                           9|    nan|eat good and exer...|                 1|    1|         eat faster |                   1|                    1|         3|         3|          1|       1|               5|        profesor |      Arabic cuisine|                3|       1|  rice  and chicken |    2|        5|          2|         5|              2|     looks not oily |      being healthy |               8|     5|          5|           5|             1|             1|rice, chicken,  soup|               1|          unemployed|                5|            1|           1|           2|           5|                     3|   1|     1|        1|             1165|            345| car racing|          5|       1|           1315|                 187|
|          3.654|     1|                   1|             610|           3|           420|     2|chocolate, chips,...|Stress, bored, anger|                          1|   3|                           1|      1|I eat about three...|                 2|    2|I eat out more th...|                   1|                    2|         2|         2|          4|       1|               2|   Self employed |             Italian|                1|       1|chicken and biscu...|    1|        4|          4|         4|              5|Grains, Veggies, ...|Try to eat 5-6 sm...|               3|     4|          4|           4|             1|             2|Pasta, steak, chi...|               4|           Nurse RN |                4|            1|           1|           4|           4|                     3|   1|     1|        2|              725|            690|Basketball |          4|       2|            900|                 155|
|            3.3|     1|                   1|             720|           4|           420|     2|frozen yogurt, pi...|     stress, sadness|                          1|   1|                           1|      3|toast and fruit f...|                 3|    1|sometimes choosin...|                   1|                    3|         2|         3|          5|       2|               2|    owns business|             italian|                1|       3|mac and cheese, p...|    1|        5|          3|         5|              6|usually includes ...|i would say my id...|               6|     6|          5|           5|             7|             2|chicken and rice ...|               2|       owns business|                4|            2|           1|           3|           5|                     6|   1|     2|        5|             1165|            500|       none|          5|       1|            900|I'm not answering...|
|            3.2|     1|                   1|             430|           3|           420|     2|Pizza, Mac and ch...|             Boredom|                          2|   2|                           2|      2|College diet, che...|                 2|    2|Accepting cheap a...|                   1|                    3|         2|         3|          5|       3|               2|        Mechanic |            Turkish |                3|       1|Beef stroganoff, ...|    2|        4|          4|         5|              7|Fresh fruits& veg...|Healthy, fresh ve...|               2|     6|          5|           5|             2|             2|    Grilled chicken |            null|                null|             null|         null|        null|        null|        null|                  null|null|  null|     null|             null|           null|       null|       null|    null|           null|                null|
| Stuffed Shells|  null|                null|            null|        null|          null|  null|                null|                null|                       null|null|                        null|   null|                null|              null| null|                null|                null|                 null|      null|      null|       null|    null|            null|             null|                null|             null|    null|                null| null|     null|       null|      null|           null|                null|                null|            null|  null|       null|        null|          null|          null|                null|            null|                null|             null|         null|        null|        null|        null|                  null|null|  null|     null|             null|           null|       null|       null|    null|           null|                null|
|Homemade Chili"|     4|Special Education...|               2|           1|             1|     2|                   5|                   5|                          1|   2|                           5|    725|                 690|               nan|    3|                   1|                1315|        Not sure, 240|      null|      null|       null|    null|            null|             null|                null|             null|    null|                null| null|     null|       null|      null|           null|                null|                null|            null|  null|       null|        null|          null|          null|                null|            null|                null|             null|         null|        null|        null|        null|                  null|null|  null|     null|             null|           null|       null|       null|    null|           null|                null|
|            3.5|     1|                   1|             720|           2|           420|     2|Ice cream, chocol...|Stress, boredom, ...|                          1|   1|                           1|      2|I try to eat heal...|                 2|    2|I have eaten gene...|                   3|                    4|         2|         2|          4|       1|               4|               IT|            Italian |                1|       3|Pasta, chicken te...|    1|        4|          4|         4|              6|A lean protein su...|Ideally I would l...|               2|     6|          2|           5|             1|             1|Chicken Parmesan,...|               5|Substance Abuse C...|                3|            1|           1|           4|           2|                     4|   1|     1|        4|              940|            500|   Softball|          4|       2|            760|                 190|
|           2.25|     1|                   1|             610|           3|           980|     2|Candy, brownies a...|None, i don't eat...|                          4|   3|                           4|    nan|My current diet i...|                 2|    2|Eating rice every...|                   1|                    3|         1|         3|          4|       2|               1|      Taxi Driver|             African|                6|       3|Fries, plaintain ...|    1|        2|          2|         2|              4|Requires veggies,...|My ideal diet is ...|               2|     1|          5|           5|             4|             2|Anything they'd w...|               1|        Hair Braider|                1|            1|           2|           5|           5|                     5|   1|     2|        4|              940|            345|      None.|          1|       2|           1315|                 190|
|            3.8|     2|                   1|             610|           3|           420|     2|Chocolate, ice cr...|     stress, boredom|                          1|   2|                           1|      1|I eat a lot of ch...|                 3|    1|I started eating ...|                   2|                    5|         2|         3|          5|       1|               4|       Assembler |                Thai|                4|       1|grilled chicken, ...|    1|        4|          4|         5|              4|Protein, vegetabl...|I would ideally l...|               2|     4|          5|           5|             8|             1|Grilled chicken, ...|               4|          Journalist|                4|            2|           2|           2|           5|                     4|   1|     1|        5|              940|            690|     soccer|          4|       1|           1315|                 180|
|            3.3|     1|                   1|             720|           3|           420|     1|Ice cream, cheese...|I eat comfort foo...|                          1|   3|                           1|      1|I eat a very heal...|                 1|    2|Freshmen year i a...|                   2|                    5|         2|         2|          2|       2|               3|     Business guy|Anything american...|                5|       1|chicken, cheesey ...|    1|        5|          2|         3|              3|A healthy meal ha...|My ideal diet is ...|               2|     5|          1|           3|             3|             1|chicken, steak, p...|               2|                cook|                4|            1|           1|           5|           1|                     3|   1|     2|        1|              725|            500|       none|          4|       2|           1315|                 137|
+---------------+------+--------------------+----------------+------------+--------------+------+--------------------+--------------------+---------------------------+----+----------------------------+-------+--------------------+------------------+-----+--------------------+--------------------+---------------------+----------+----------+-----------+--------+----------------+-----------------+--------------------+-----------------+--------+--------------------+-----+---------+-----------+----------+---------------+--------------------+--------------------+----------------+------+-----------+------------+--------------+--------------+--------------------+----------------+--------------------+-----------------+-------------+------------+------------+------------+----------------------+----+------+---------+-----------------+---------------+-----------+-----------+--------+---------------+--------------------+
```

# Selecionando os dados

   1. Selecionar as colunas e linhas a serem utilizadas na análise.


```python
df2 = df_food.select('Gender', 'type_sports', 'mother_profession', 'father_profession','marital_status','weight')
```


```python
+------+-----------+--------------------+-----------------+--------------+--------------------+
|Gender|type_sports|   mother_profession|father_profession|marital_status|              weight|
+------+-----------+--------------------+-----------------+--------------+--------------------+
|     2| car racing|          unemployed|        profesor |             1|                 187|
|     1|Basketball |           Nurse RN |   Self employed |             2|                 155|
|     1|       none|       owns business|    owns business|             2|I'm not answering...|
|     1|       null|                null|        Mechanic |             2|                null|
|  null|       null|                null|             null|          null|                null|
|     4|       null|                null|             null|          null|                null|
|     1|   Softball|Substance Abuse C...|               IT|             1|                 190|
|     1|      None.|        Hair Braider|      Taxi Driver|             2|                 190|
|     2|     soccer|          Journalist|       Assembler |             1|                 180|
|     1|       none|                cook|     Business guy|             1|                 137|
+------+-----------+--------------------+-----------------+--------------+--------------------+
```

# Limpando os Dados

   1. Limpar valores que contém: None., none e nan.
   2. Limpar valores nulos em cada uma das colunas a serem utilizadas.


```python
df2 = df2.filter((df_food[0] != 'none') & (df_food[0] != 'None.') & (df_food[0] != 'nan'))
```


```python
df2 = df2.filter(df_food[0].isNotNull())
df2 = df2.filter(df_food[1].isNotNull())
df2 = df2.filter(df_food[2].isNotNull())
df2 = df2.filter(df_food[3].isNotNull())
df2 = df2.filter(df_food[4].isNotNull())
df2 = df2.filter(df_food[5].isNotNull())
```


```python
+------+-----------+--------------------+--------------------+--------------+--------------------+
|Gender|type_sports|   mother_profession|   father_profession|marital_status|              weight|
+------+-----------+--------------------+--------------------+--------------+--------------------+
|     2| car racing|          unemployed|           profesor |             1|                 187|
|     1|Basketball |           Nurse RN |      Self employed |             2|                 155|
|     1|       none|       owns business|       owns business|             2|I'm not answering...|
|     1|       null|                null|           Mechanic |             2|                null|
|     4|       null|                null|                null|          null|                null|
|     1|   Softball|Substance Abuse C...|                  IT|             1|                 190|
|     1|      None.|        Hair Braider|         Taxi Driver|             2|                 190|
|     2|     soccer|          Journalist|          Assembler |             1|                 180|
|     1|       none|                cook|        Business guy|             1|                 137|
|     1|       none|Elementary School...|High School Princ...|             2|                 180|
+------+-----------+--------------------+--------------------+--------------+--------------------+
```

# Criando colunas

   1. Criar coluna de conversão de libras para kilogrmas. 1lb = 0.045kg.
   2. Criar coluna com a descrição do genêro.


```python
df2 = df2.withColumn('PesoKG', lit(col('weight') * 0.45))
```


```python
df2 = df2.withColumn('Genero', when(df2.Gender == "1", "Masculino")
                        .when(df2.Gender == "2", "Feminino")
                        .otherwise('Não Identificado'))
```


```python
df_food.select('type_sports', when(df_food.weight >= 150, 'Sobrepeso'))
```




    DataFrame[type_sports: string, CASE WHEN (weight >= 150) THEN Sobrepeso END: string]




```python
+------+-----------+--------------------+--------------------+--------------+--------------------+------+----------------+
|Gender|type_sports|   mother_profession|   father_profession|marital_status|              weight|PesoKG|          Genero|
+------+-----------+--------------------+--------------------+--------------+--------------------+------+----------------+
|     2| car racing|          unemployed|           profesor |             1|                 187| 84.15|        Feminino|
|     1|Basketball |           Nurse RN |      Self employed |             2|                 155| 69.75|       Masculino|
|     1|       none|       owns business|       owns business|             2|I'm not answering...|  null|       Masculino|
|     1|       null|                null|           Mechanic |             2|                null|  null|       Masculino|
|     4|       null|                null|                null|          null|                null|  null|Não Identificado|
|     1|   Softball|Substance Abuse C...|                  IT|             1|                 190|  85.5|       Masculino|
|     1|      None.|        Hair Braider|         Taxi Driver|             2|                 190|  85.5|       Masculino|
|     2|     soccer|          Journalist|          Assembler |             1|                 180|  81.0|        Feminino|
|     1|       none|                cook|        Business guy|             1|                 137| 61.65|       Masculino|
|     1|       none|Elementary School...|High School Princ...|             2|                 180|  81.0|       Masculino|
+------+-----------+--------------------+--------------------+--------------+--------------------+------+----------------+
```

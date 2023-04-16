data = [('EMP001','Loki','CS','M',30000,'N'),
        ('EMP001','Loki','IT','M',40000,'Y'),
      ('EMP002','Robert','EE','M',40000,'Y'),
         ('EMP003','william','IT','M',40000,'Y'),
         ('EMP004','Rana','ME','M',230000,'Y'),
        ('EMP005','Luke','IT','M',55000,'N'),
        ('EMP005','Luke','EC','M',60000,'Y'),]

columns = ["empid","name","department","gender","salary","current_version"]
df1 = spark.createDataFrame(data=data, schema = columns)

import pyspark.sql.functions as F
natural_key_cols = ["empid","name"]
duplicated_natural_keys = df1.
groupby(natural_key_cols).
count().
filter(F.col('count') > 1).
drop(F.col('count'))

df1.join(F.broadcast(duplicated_natural_keys), natural_key_cols).
orderBy(natural_key_cols).display()
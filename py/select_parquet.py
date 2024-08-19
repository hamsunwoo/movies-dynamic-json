from pyspark.sql import SparkSession
import sys

# Spark 세션 시작
spark = SparkSession.builder.appName("select").getOrCreate()
logFile = "/Users/seon-u/app/spark-3.5.1-bin-hadoop3/README.md"  # Should be some file on your system
logData = spark.read.text(logFile).cache()

year = sys.argv[1]

#parquet 데이터 읽기
df = spark.read.parquet(f"/Users/seon-u/data/movies_pagelimit/parquet/year={year}")
df.createOrReplaceTempView("pars")


#sql 
df1 = spark.sql("""
                SELECT
                    peopleNm,
                    count(peopleNm) as count
                FROM pars
                GROUP BY peopleNm
                """
                )


df2 = spark.sql("""
                SELECT
                    companyNm,
                    count(companyNm) as count
                FROM pars
                GROUP BY companyNm
                """
                )


#sql문을 다시 테이블로 만들기
df1.createOrReplaceTempView("director")
df2.createOrReplaceTempView("company")

df1.show()
df2.show()
spark.stop()

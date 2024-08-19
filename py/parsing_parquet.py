from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, size, explode_outer
from pyspark.sql.types import StructType, ArrayType


# Spark 세션 시작
spark = SparkSession.builder.appName("parsing").getOrCreate()

year = sys.args[1]


# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json(f'/Users/seon-u/data/movies_pagelimit/year={year}/data.json')

def get_json_keys(schema, prefix):
    keys=[]
    for field in schema.fields:
        if isinstance(field.dataType, StructType):
            if prefix:
                new_prefix=f"{prefix}.{field.name}"
            else:
                new_prefix=field.name
            keys+=get_json_keys(field.dataType, new_prefix)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            if prefix:
                new_prefix=f"{prefix}.{field.name}"
            else:
                new_prefix=field.name
            keys+=get_json_keys(field.dataType.elementType, new_prefix)
        else:
            if prefix:
                keys.append(f"{prefix}.{field.name}")
            else:
                keys.append(field.name)

    return keys

#companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

#펼치기
edf = ccdf.withColumn("company",  explode_outer("companys"))

#또 펼치기
eedf = edf.withColumn("director",  explode_outer("directors"))

#eedf의 schema 모두 출력
filter_tool=get_json_keys(eedf.schema,"")

#필요없는 컬럼 삭제
filter_tool.remove('companys.companyCd')
filter_tool.remove('directors.peopleNm')
table = eedf.select(*filter_tool)
table.show()

#테이블 생성
table.createOrReplaceTempView("pars")

#sql을 이용하여 집계
parsing = spark.sql("""
                    SELECT
                        peopleNm,
                        count(distinct peopleNm) as count
                    FROM pars
                    GROUP BY peopleNm
                    """)

parsing.show()

spark.stop()

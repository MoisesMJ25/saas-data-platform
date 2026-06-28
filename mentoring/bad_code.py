import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def process(file_path, country):
    df = pd.read_csv(file_path)
    df = df[df["pais"] == country]
    result = []
    for i, row in df.iterrows():
        if row["tipo_entrega"] == "ZPRE" or row["tipo_entrega"] == "ZVE1":
            if row["unidad"] == "CS":
                qty = row["cantidad"] * 20
            else:
                qty = row["cantidad"]
            result.append({
                "pais": row["pais"],
                "fecha": row["fecha_proceso"],
                "material": row["material"],
                "cantidad_st": qty,
                "total": qty * row["precio"],
            })
    out = pd.DataFrame(result)
    sdf = spark.createDataFrame(out)
    sdf.write.mode("overwrite").parquet("/tmp/output/" + country)
    print("done")
    return out


process("data.csv", "GT")

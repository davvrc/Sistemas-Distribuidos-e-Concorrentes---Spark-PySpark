import os, time, math, random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, window, lit, rand

LIMITE = 40.0                       # média máxima segura (°C)
ZONAS = {1: "Forjaria", 2: "Soldagem", 3: "Montagem", 4: "Expedição"}

# Cada zona esquenta e esfria sozinha, de forma ALEATÓRIA (muda a cada execução):
# temperatura = base + amplitude * onda, com base, amplitude, período e fase sorteados.
base = [random.uniform(25, 31) for _ in ZONAS]        # °C em repouso
amp = [random.uniform(4, 14) for _ in ZONAS]          # quanto a zona pode esquentar
periodo = [random.uniform(35, 70) for _ in ZONAS]     # segundos para um ciclo completo
fase = [random.uniform(0, 2 * math.pi) for _ in ZONAS]

# forçar o pico entre 40 s e 70 s depois do início.
critica = random.randrange(len(ZONAS))
amp[critica] = random.uniform(18, 22)
fase[critica] = math.pi / 2 - 2 * math.pi * random.uniform(40, 70) / periodo[critica]

# [SPARK] 1) Sessão: "local[4]" simula um cluster com 4 workers. Conceitos de Cluster Manager e Spark Session.
spark = (SparkSession.builder.appName("MonitorSimples").master("local[4]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
inicio = time.time()

def por_zona(valores):
    """Vira uma coluna Spark: o valor de cada zona escolhido pelo número da zona."""
    return F.element_at(F.array(*[lit(v) for v in valores]), col("zona"))

# [SPARK] 2) Fluxo de leituras simuladas: 1000 leituras/s de 100 sensores. Essa parte do codigo é onde entra em pratica a parte simulada do projeto.
# "spark.readStream.format("rate").option("rowsPerSecond", 1000)" Gera 1000 linhas de dados que são distribuidos entre os 100 sensores. 
# Simulando os dados extraidos dos sensores espalhados na fabrica em um cenario hipotetico. 
sensores = (spark.readStream.format("rate").option("rowsPerSecond", 1000).load()
            .withColumn("sensor_id", col("value") % 100)
            .withColumn("zona", (col("sensor_id") % 4 + 1).cast("int"))
            .withColumn("segundos", col("timestamp").cast("double") - lit(inicio)))

onda = (F.sin(col("segundos") * 2 * math.pi / por_zona(periodo) + por_zona(fase)) + 1) / 2  # 0..1
leituras = sensores.withColumn(
    "temperatura", por_zona(base) + por_zona(amp) * onda + (rand() - 0.5) * 2)

# [SPARK] 3) Estatísticas por zona numa janela deslizante de 30 s (passo de 5 s). Conceito de Janela Deslizante (30s) aplicada. Fast Data.
# Stateful Streaming. Dados são acessados em velocidade de memoria RAM, por isso consegue manter esse fluxo de processamento tao rapido. 
# ".agg(F.avg("temperatura").alias("media")" Fase de Shuffle. Spark reorganiza os dados com base nas zonas, e enviando pro worker responsavel. 
medias = (leituras
          .withWatermark("timestamp", "10 seconds")                       # tolera dados atrasados
          .groupBy(window("timestamp", "30 seconds", "5 seconds").alias("janela"), "zona")
          .agg(F.avg("temperatura").alias("media"),
               F.min("temperatura").alias("minima"),
               F.max("temperatura").alias("maxima"),
               F.count("*").alias("leituras"),
               F.approx_count_distinct("sensor_id").alias("sensores")))

# [SPARK] 4) Sink "memory": o resultado vira uma tabela SQL, consultável enquanto o fluxo roda. Isso só é possivel graças ao RDD e o conceito de 
# Processar dados a partir da memoria RAM ao invés de leituras/escritas em disco rigidos. 
consulta = (medias.writeStream.queryName("medias_zonas")
            .format("memory").outputMode("complete").start())

# 5) A cada 3 s consultamos a tabela com SQL (o status é calculado pelo Spark: CASE WHEN)
try:
    while True:
        time.sleep(3)
        linhas = spark.sql(f"""
            SELECT zona, sensores, leituras,
                   ROUND(media, 1) AS media, ROUND(minima, 1) AS minima, ROUND(maxima, 1) AS maxima,
                   CASE WHEN media > {LIMITE}     THEN 'SUPERAQUECIMENTO'
                        WHEN media > {LIMITE - 3} THEN 'ATENCAO'
                        ELSE 'NORMAL' END AS status
            FROM medias_zonas
            WHERE janela.end = (SELECT MIN(janela.end) FROM medias_zonas
                                WHERE janela.end > current_timestamp())
            ORDER BY zona""").collect()
        print(f"\n=== {time.time() - inicio:4.0f}s | média dos últimos 30 s | limite {LIMITE:.0f} °C ===")
        print(f"{'ZONA':<11}{'SENS':>5}{'LEIT':>7}{'MÉDIA':>8}{'MÍN':>7}{'MÁX':>7}  STATUS")
        for l in linhas:
            alerta = "🔥 " if l.status == "SUPERAQUECIMENTO" else "⚠  " if l.status == "ATENCAO" else "   "
            print(f"{ZONAS[l.zona]:<11}{l.sensores:>5}{l.leituras:>7}{l.media:>7.1f}°"
                  f"{l.minima:>6.1f}°{l.maxima:>6.1f}°  {alerta}{l.status}")
except KeyboardInterrupt:
    consulta.stop()
    spark.stop()

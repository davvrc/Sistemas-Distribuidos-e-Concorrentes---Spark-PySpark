# Monitoramento Térmico de Fábrica em Tempo Real — PySpark

Projeto da Unidade 05 (Ferramentas de Big Data — **Apache Spark**).
Simula, em Python, um cluster Spark processando em tempo real leituras de **400 sensores IoT**
de uma fábrica (8 zonas). O Spark calcula a **média por zona numa janela deslizante** e dispara
um **alerta de superaquecimento** quando a média passa do limite seguro.

## Como executar

Requisitos: **Python 3.9+** e **Java 8/11/17/21** (o Spark roda na JVM).

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
python monitor_termico.py
```

Dica para apresentar: use terminal com **fundo escuro**, fonte grande e a janela maximizada
(o painel tem ~95 colunas). Para esconder os avisos de inicialização do Spark:
`python monitor_termico.py 2>/dev/null` (Windows PowerShell: `2>$null`).

Durante a execução, abra **http://localhost:4040** (Spark UI) — as abas *Jobs*, *Stages* e
*Structured Streaming* mostram o "cluster" trabalhando. Ótimo para a apresentação.

### Opções

| Opção | Padrão | O que faz |
|---|---|---|
| `--master` | `local[4]` | "Cluster": 4 workers (threads). Ex.: `local[2]`, `local[*]` |
| `--sensores` | 400 | Total de sensores (múltiplo de 8) |
| `--taxa` | 1000 | Leituras por segundo |
| `--janela` / `--slide` | 30 / 5 | Janela deslizante e passo, em segundos |
| `--limite` | 40 | Média máxima segura (°C) |
| `--duracao` | 150 | Segundos de simulação (0 = até Ctrl+C) |
| `--sem-limpar` | — | Não limpa a tela (útil para gravar o log) |

> **Sobre os "5 minutos":** para a demo caber em ~2,5 min, o tempo foi comprimido (janela de 30 s).
> Para a versão real: `python monitor_termico.py --janela 300 --slide 30 --duracao 0`.

## O que acontece na simulação (roteiro de ~2,5 min)

| Tempo | Evento |
|---|---|
| 0–20 s | Todas as zonas **NORMAL** (primeira janela ainda sendo preenchida) |
| ~20 s | **Forjaria** começa a esquentar (+0,8 °C/s) |
| ~27 s | Forjaria entra em **ATENÇÃO** (média > 37 °C) |
| ~31 s | Forjaria passa de 40 °C → **🔥 SUPERAQUECIMENTO** + banner vermelho + log |
| ~75 s | **Estufa de Pintura** sobe devagar e chega a **ATENÇÃO** (sem alerta) |
| 100 s | Forjaria é "resfriada" (problema resolvido) |
| ~117 s | Média volta ao limite → **NORMALIZOU** no log |

A média é uma **janela**, então ela *atrasa* em relação à temperatura real — isso é proposital e
é um bom ponto para explicar (suaviza picos/ruído de sensor, mas reage com alguns segundos de atraso).

## Mapa dos conceitos do Spark no código

Cada item está marcado no código com `# [SPARK]` e numerado de 1 a 9.

| Seção | Conceito | Onde aparece / o que explicar |
|---|---|---|
| 1 | **SparkSession** e `master` | Ponto de entrada; `local[4]` simula 4 workers. Em produção seria YARN/Dataproc |
| 2 | **Structured Streaming** (`readStream`, fonte `rate`) | DataFrame *ilimitado*; processado em **micro-lotes** (mesma fonte do exemplo 6.5.5 do PDF) |
| 3 | **Broadcast join** (stream × tabela estática) | Tabela de zonas enviada a todos os workers, sem shuffle |
| 3 | **Transformações em colunas** (sem UDF) | Rodam na JVM, otimizadas pelo Catalyst/Tungsten; *lazy evaluation* |
| 4 | **Janela deslizante** `window(ts, 30s, 5s)` | Janelas sobrepostas = "média dos últimos N s" |
| 4 | **Watermark** `withWatermark` | Tolera eventos atrasados e libera estado antigo da memória |
| 4 | **groupBy / agg + shuffle** | Dados reagrupados por (janela, zona) entre os workers; `approx_count_distinct` |
| 5 | **Regra de alerta** `when/otherwise` | A *flag* `superaquecimento` é calculada pelo Spark |
| 6 | **foreachBatch** + `collect()` | Cada micro-lote vira um DataFrame; `collect()` é a **ação** que dispara o job |
| 7 | **outputMode("update")**, **trigger**, **checkpoint** | Latência (2 s), saída incremental e tolerância a falhas |
| 9 | `query.stop()` / `spark.stop()` | Encerramento limpo |

Também vale citar: `spark.sql.shuffle.partitions` (ajustado de 200 para 8) e `numPartitions`
na fonte (geração paralela dos dados).

## Relação com o material de apoio

- **PDF Unidade 05, seção 6.5.5** — mesmo esqueleto: `readStream.format("rate")`, `withWatermark`,
  `groupBy(window(...))`, `writeStream`. Diferenças: aqui usamos janela deslizante, `update`
  (em vez de `append`) e `foreachBatch` para o painel.
- **Spark docs** (https://spark.apache.org/docs/latest/) — *Structured Streaming Programming Guide*:
  window operations, watermarking, output modes, foreachBatch.
- **Hadoop/Dataproc** — o mesmo código roda num cluster real mudando só o `--master` (ou via
  `spark-submit` no Google Cloud Dataproc).

## Sugestão de falas para a apresentação

1. **Problema:** centenas de sensores × leituras contínuas = fluxo grande demais para um único processo.
2. **Solução:** Spark divide o fluxo entre workers (partições) e junta o resultado (shuffle).
3. **Mostrar** o painel e, ao mesmo tempo, a Spark UI.
4. **Apontar** o momento do alerta e explicar a janela deslizante (por que não alerta instantaneamente).
5. **Fechar** com escalabilidade: trocar `local[4]` por um cluster de dezenas de nós sem alterar a lógica.

## Limitações (honestidade para a banca)

- Os dados são **simulados** (fonte `rate`), não vêm de sensores reais; em produção a fonte seria
  **Kafka** (`readStream.format("kafka")`) — só essa linha mudaria.
- `local[4]` simula o cluster em **uma única máquina**; o modelo de execução é o mesmo, mas não há rede entre nós.
- O PDF cita *Storm* e *Flink* para streaming; aqui usamos o *Structured Streaming* do Spark
  (micro-lotes), que é o recurso do Spark para fluxo contínuo.


from pyspark.sql import functions as F, Window
import re


CATALOGO = "main"
VOLUMEN  = "/Volumes/main/raw/betalpes_csv"   
SEP_PUNTOYCOMA = False                       


BRONZE = f"`{CATALOGO}`.`bronze`"
SILVER = f"`{CATALOGO}`.`silver`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.silver")

def _ls_volume_csv(volume_path):
    try:
        files = dbutils.fs.ls(volume_path)
    except Exception as e:
        raise RuntimeError(f"No se puede listar el Volume {volume_path}: {e}")
    return {f.name.lower(): f.path for f in files if f.name.lower().endswith(".csv")}

ARCHIVOS = _ls_volume_csv(VOLUMEN)

def _leer_csv(nombre):
    llave = f"{nombre}.csv".lower()
    if llave not in ARCHIVOS:
        raise FileNotFoundError(f"{nombre}.csv no esta en {VOLUMEN}. Encontrados: {list(ARCHIVOS.keys())}")
    reader = (spark.read.option("header", True).option("inferSchema", True))
    if SEP_PUNTOYCOMA:
        reader = reader.option("sep",";")
    return reader.csv(ARCHIVOS[llave])

def _guardar(schema, nombre, df):
    df.write.mode("overwrite").format("delta").saveAsTable(f"`{CATALOGO}`.`{schema}`.`{nombre}`")
    n = spark.table(f"`{CATALOGO}`.`{schema}`.`{nombre}`").count()
    print(f"{CATALOGO}.{schema}.{nombre} -> {n} filas")

# ---------- BRONZE ----------
for t in ["match","team","player","player_attributes","team_attributes","league","country"]:
    try:
        _guardar("bronze", t, _leer_csv(t))
    except Exception as e:
        print(f"bronze.{t} omitida: {e}")

b = lambda t: spark.table(f"{BRONZE}.{t}")

# ---------- SILVER ----------

# match: fecha y season
if spark.catalog.tableExists(f"{BRONZE}.match"):
    m = b("match")
    date_col = next((c for c in m.columns if c.lower()=="date"), None)
    if date_col:
        m = m.withColumn("match_date", F.to_timestamp(F.col(date_col)))
    else:
        m = m.withColumn("match_date", F.lit(None).cast("timestamp"))
    if "season" in m.columns:
        m = (m.withColumn("season_start", F.split("season","/").getItem(0).cast("int"))
               .withColumn("season_end",   F.split("season","/").getItem(1).cast("int")))
    _guardar("silver", "match", m)

# team: 1:1
if spark.catalog.tableExists(f"{BRONZE}.team"):
    _guardar("silver", "team", b("team"))

# player: birthday -> date
if spark.catalog.tableExists(f"{BRONZE}.player"):
    p = b("player")
    bday = next((c for c in p.columns if c.lower()=="birthday"), None)
    if bday:
        p = p.withColumn(bday, F.to_date(F.col(bday)))
    _guardar("silver", "player", p)

# player_attributes: attr_date + dropna estricto 
if spark.catalog.tableExists(f"{BRONZE}.player_attributes"):
    pa = b("player_attributes")
    dcol = next((c for c in pa.columns if c.lower()=="date"), None)
    if dcol:
        pa = pa.withColumn("attr_date", F.to_timestamp(F.col(dcol)))
    pa = pa.dropna(how="any")
    _guardar("silver", "player_attributes", pa)

# team_attributes: dedupe conserva el menor id
if spark.catalog.tableExists(f"{BRONZE}.team_attributes"):
    ta = b("team_attributes")
    if "id" in ta.columns:
        w = Window.partitionBy([c for c in ta.columns if c!="id"]).orderBy(F.col("id").asc_nulls_last())
        ta = ta.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn")==1).drop("_rn")
    else:
        ta = ta.dropDuplicates()
    _guardar("silver", "team_attributes", ta)

# league & country: 1:1
for t in ["league","country"]:
    if spark.catalog.tableExists(f"{BRONZE}.{t}"):
        _guardar("silver", t, b(t))

# events_long: detecta prefijos H/D/A parte de camilo
if spark.catalog.tableExists(f"{SILVER}.match"):
    m = spark.table(f"{SILVER}.match")
    cols = m.columns
    norm = {re.sub(r"[ _]+","",c).lower(): c for c in cols}  
    grupos = {}
    for k, orig in norm.items():
        mobj = re.match(r"^([a-z0-9]+)(h|d|a)$", k)
        if mobj:
            pref, suf = mobj.group(1).upper(), mobj.group(2)
            grupos.setdefault(pref, {})[suf] = orig
    triples = [(pref, d["h"], d["d"], d["a"]) for pref,d in grupos.items() if {"h","d","a"}.issubset(d.keys())]
    if not triples:
        print("No se detectaron tripletas H/D/A en match; events_long no se creará.")
    else:
        arrs = [F.struct(F.lit(pref).alias("betting_office"),
                         F.col(h).cast("double").alias("home_odd"),
                         F.col(d).cast("double").alias("draw_odd"),
                         F.col(a).cast("double").alias("visitor_odd"))
                for (pref,h,d,a) in triples]
        ev = (m.select(
                (F.col("id").alias("id_match") if "id" in cols else F.monotonically_increasing_id().alias("id_match")),
                (F.col("league_id").alias("id_league") if "league_id" in cols else F.lit(None).cast("int").alias("id_league")),
                F.to_date("match_date").alias("date_key"),
                F.array(*arrs).alias("events"))
              .withColumn("ev", F.explode("events"))
              .select("id_match","id_league","date_key",
                      F.col("ev.betting_office").alias("betting_office"),
                      "ev.home_odd","ev.draw_odd","ev.visitor_odd"))
        _guardar("silver", "events_long", ev)

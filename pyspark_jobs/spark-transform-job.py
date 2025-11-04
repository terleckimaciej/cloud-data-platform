import os
from datetime import date, datetime
from pyspark.sql import SparkSession, functions as F

# === KONFIG ===
PROJECT_ID = "pracuj-pl-pipeline"
BUCKET = "pracuj-pl-data-lake"
TODAY = date.today().isoformat()

INPUT_PATH = f"gs://{BUCKET}/enriched/job_details_enriched_{TODAY}.parquet"
OUTPUT_PATH = f"gs://{BUCKET}/curated/{TODAY}/"

# === 1️⃣ Inicjalizacja Sparka ===
spark = (
    SparkSession.builder
    .appName("Job Details Transformation Dev")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

print("✅ Spark session started")
print("Spark version:", spark.version)
print("Input path:", INPUT_PATH)
print("Output path:", OUTPUT_PATH)

# === 2️⃣ Wczytanie danych ===
df = spark.read.parquet(INPUT_PATH)

# === 2a️⃣ Wyodrębnienie unikalnego ID oferty z URL ===
df = df.withColumn("offer_id", F.regexp_extract(F.col("url"), r",oferta,(\d+)", 1))

# === 3️⃣ Czyszczenie nazw firm ===
df = df.withColumn(
    "company_name",
    F.trim(F.regexp_replace(F.col("company_name"), "( O firmie| About the company)", ""))
)

# === 4️⃣ Mapy miesięcy PL + EN ===
month_map = {
    "sty": 1, "lut": 2, "mar": 3, "kwi": 4, "maj": 5, "cze": 6,
    "lip": 7, "sie": 8, "wrz": 9, "paź": 10, "paz": 10, "lis": 11, "gru": 12,
    "jan": 1, "feb": 2, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}
month_map_expr = F.create_map([F.lit(x) for x in sum(month_map.items(), ())])

today = datetime.today()
current_year = today.year
current_month = today.month


# === 5️⃣ Kolumna valid_until ===
df = (
    df
    .withColumn("valid_until_clean", F.lower(F.col("valid_until_raw")))
    .withColumn("inner_date", F.regexp_extract(F.col("valid_until_clean"), r"\(\s*(?:do|to)\s*(\d{1,2})\s*([a-ząćęłńóśźż]{3,})\s*\)", 0))
    .withColumn("day_int", F.regexp_extract("inner_date", r"(\d{1,2})", 1).cast("int"))
    .withColumn("month_str", F.regexp_extract("inner_date", r"([a-ząćęłńóśźż]{3,})", 1))
    .withColumn("month_num", month_map_expr[F.col("month_str")])
    .withColumn("year_final",
                F.when(F.col("month_num") < F.lit(current_month), F.lit(current_year + 1))
                 .otherwise(F.lit(current_year)))
    .withColumn("valid_until", F.to_date(F.concat_ws("-", F.col("year_final"),
                                                    F.lpad(F.col("month_num").cast("string"), 2, "0"),
                                                    F.lpad(F.col("day_int").cast("string"), 2, "0"))))
    .drop("valid_until_clean", "inner_date", "day_int", "month_str", "month_num", "year_final", "valid_until_raw")
)

from pyspark.sql import functions as F

# === Słownik miesięcy (pełne polskie nazwy) ===
month_map_full = {
    "stycznia": 1, "lutego": 2, "marca": 3, "kwietnia": 4,
    "maja": 5, "czerwca": 6, "lipca": 7, "sierpnia": 8,
    "września": 9, "października": 10, "pazdziernika": 10,
    "listopada": 11, "grudnia": 12
}
month_map_expr_full = F.create_map([F.lit(x) for x in sum(month_map_full.items(), ())])

# === Poprawiony kod ===
df = (
    df
    .withColumn("date_added_clean", F.lower(F.col("date_added_raw")))
    # dzień
    .withColumn("day_int", F.regexp_extract("date_added_clean", r"(\d{1,2})", 1).cast("int"))
    # miesiąc (pełna nazwa, polskie znaki i opcjonalna spacja przed)
    .withColumn("month_str", F.regexp_extract("date_added_clean", r"\d{1,2}\s+([a-zęóąśłżźćń]+)", 1))
    # rok
    .withColumn("year_int", F.regexp_extract("date_added_clean", r"(\d{4})", 1).cast("int"))
    # dopasowanie do mapy
    .withColumn("month_num", month_map_expr_full[F.col("month_str")])
    # składanie poprawnej daty
    .withColumn(
        "date_added",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("year_int"),
                F.lpad(F.col("month_num").cast("string"), 2, "0"),
                F.lpad(F.col("day_int").cast("string"), 2, "0")
            )
        )
    )
    .drop("date_added_clean", "day_int", "month_str", "month_num", "year_int", "date_added_raw")
)

df.select("date_added").show(truncate=False)

from pyspark.sql import functions as F

df = (
    df
    # --- czyszczenie tekstu ---
    .withColumn("salary_clean", F.lower(F.col("salary_raw")))
    .withColumn("salary_clean", F.regexp_replace("salary_clean", "–", "-"))       # en dash → minus
    .withColumn("salary_clean", F.regexp_replace("salary_clean", ",", "."))       # przecinek → kropka
    .withColumn("salary_clean", F.regexp_replace("salary_clean", "zł", ""))
    # 🔥 usuń wszystkie spacje (normalne + niełamliwe + wąskie itp.)
    .withColumn("salary_clean", F.regexp_replace("salary_clean", "[\\u00A0\\u202F\\s]+", ""))
    # --- teraz można bezpiecznie splitować ---
    .withColumn("salary_parts", F.split(F.col("salary_clean"), "-"))

    # --- salary_min / max / avg ---
    .withColumn("salary_min",
                F.when(F.size("salary_parts") >= 1,
                       F.regexp_extract(F.col("salary_parts")[0], r"(\d+(?:\.\d+)?)", 1).cast("double")))
    .withColumn("salary_max",
                F.when(F.size("salary_parts") >= 2,
                       F.regexp_extract(F.col("salary_parts")[1], r"(\d+(?:\.\d+)?)", 1).cast("double"))
                 .otherwise(F.col("salary_min")))
    .withColumn("salary_avg", (F.col("salary_min") + F.col("salary_max")) / 2)

    # --- salary_type ---
    .withColumn("salary_type",
                F.when(F.col("salary_min").isNull(), "none")
                 .when(F.col("salary_min") == F.col("salary_max"), "single")
                 .otherwise("range"))

    # --- salary_period ---
    .withColumn("unit_lower", F.lower(F.col("salary_unit_raw")))
    .withColumn(
        "salary_period",
        F.coalesce(
            *[F.when(F.col("unit_lower").contains(k), F.lit(v)) for k, v in {
                "godz": "hour", "hr": "hour", "hour": "hour",
                "mth": "month", "mies": "month", "month": "month",
                "year": "year", "rok": "year"
            }.items()],
            F.lit("unknown")
        )
    )

    # --- salary_equiv_monthly ---
    .withColumn(
        "salary_equiv_monthly",
        F.when(F.col("salary_period") == "hour", F.col("salary_avg") * 160)
         .when(F.col("salary_period") == "year", F.col("salary_avg") / 12)
         .otherwise(F.col("salary_avg"))
    )

    .drop("salary_clean", "salary_parts", "unit_lower")
)

df.select(
    "salary_raw", "salary_unit_raw", "salary_min", "salary_max", "salary_avg",
    "salary_type", "salary_period", "salary_equiv_monthly"
).show(truncate=False)

from pyspark.sql import functions as F

df = (
    df
    .withColumn("work_lower", F.lower(F.col("work_schedule")))
    .withColumn(
        "work_schedule",
        F.when(F.col("work_lower").isNull(), "unknown")
         .when(F.col("work_lower").rlike("pełny etat|full[- ]?time"), "full_time")
         .when(F.col("work_lower").rlike("część etatu|part[- ]?time"), "part_time")
         .when(F.col("work_lower").rlike("dodatkowa|tymczasowa|temporary|additional"), "temporary")
         .otherwise("unknown")
    )
    .drop("work_lower")
)

from pyspark.sql import functions as F

df = (
    df
    .withColumn("contract_lower", F.lower(F.col("contract_type")))

    # główny typ (priorytetowy mapping)
    .withColumn(
        "contract_type",
        F.when(F.col("contract_lower").isNull(), "unknown")
         # etat
         .when(F.col("contract_lower").rlike("umowa o pracę|contract of employment"), "employment")
         # b2b
         .when(F.col("contract_lower").rlike("b2b"), "b2b")
         # zlecenie
         .when(F.col("contract_lower").rlike("umowa zlecenie|contract of mandate"), "mandate")
         # dzieło
         .when(F.col("contract_lower").rlike("umowa o dzieło|contract for specific work"), "contract_work")
         # staż
         .when(F.col("contract_lower").rlike("staż|internship|apprenticeship"), "internship")
         # zastępstwo
         .when(F.col("contract_lower").rlike("zastępstwo|replacement"), "replacement")
         # tymczasowa / outsourcing
         .when(F.col("contract_lower").rlike("temporary|staffing|leasing"), "temporary")
         .otherwise("unknown")
    )

    .drop("contract_lower")
)

from pyspark.sql import functions as F

rank_map = {
    "intern": 1,
    "junior": 2,
    "mid": 3,
    "senior": 4,
    "expert": 5,
    "manager": 6,
    "director": 7
}

rank_expr = F.create_map([F.lit(x) for x in sum(rank_map.items(), ())])

df = (
    df
    .withColumn("pos_lower", F.lower(F.col("position_level")))
    .withColumn(
        "position_level",
        F.when(F.col("pos_lower").isNull(), "unknown")
         .when(F.col("pos_lower").rlike("dyrektor|director"), "director")
         .when(F.col("pos_lower").rlike("kierownik|koordynator|menedżer|manager|supervisor|team lead|head of"), "manager")
         .when(F.col("pos_lower").rlike("ekspert|expert|principal|lead"), "expert")
         .when(F.col("pos_lower").rlike("starszy|senior"), "senior")
         .when(F.col("pos_lower").rlike("specjalista|specialist|mid|regular"), "mid")
         .when(F.col("pos_lower").rlike("młodszy|junior|asystent|assistant"), "junior")
         .when(F.col("pos_lower").rlike("praktykant|stażysta|trainee|intern"), "intern")
         .otherwise("unknown")
    )
    .withColumn("position_rank", rank_expr[F.col("position_level")])
    .drop("pos_lower")
)

from pyspark.sql import functions as F

df = (
    df
    .withColumn("mode_lower", F.lower(F.col("work_mode")))
    .withColumn(
        "work_mode",
        F.when(F.col("mode_lower").rlike("zdaln|home office|remote"), "remote")
         .when(F.col("mode_lower").rlike("hybryd|hybrid"), "hybrid")
         .when(F.col("mode_lower").rlike("stacjonarn|full office|office work"), "office")
         .otherwise("unknown")
    )
    .drop("mode_lower")
)

from pyspark.sql import functions as F

# --- mapowanie regionów EN -> PL ---
region_map = {
    "masovian": "mazowieckie",
    "lesser poland": "małopolskie",
    "lower silesia": "dolnośląskie",
    "pomeranian": "pomorskie",
    "greater poland": "wielkopolskie",
    "silesian": "śląskie",
    "subcarpathian": "podkarpackie",
    "lodz": "łódzkie",
    "łódź": "łódzkie",
}
region_expr = F.create_map([F.lit(x) for x in sum(region_map.items(), ())])

# --- whitelist ---
cities = [
    "warszawa","kraków","wrocław","gdańsk","poznań","łódź","szczecin","lublin","katowice","bydgoszcz",
    "gdynia","rzeszów","białystok","toruń","kielce","olsztyn","opole",
    "zielona góra","gorzów wielkopolski",
    "frankfurt","berlin","vienna"
]
city_regex = "(" + "|".join([c.replace(" ", "\\s+") for c in cities]) + ")"

# --- pełna lista regionów (PL i EN) ---
region_words = [
    "mazowieckie","małopolskie","dolnośląskie","śląskie","pomorskie","wielkopolskie","łódzkie",
    "podkarpackie","lubelskie","kujawsko-pomorskie","zachodniopomorskie","świętokrzyskie",
    "warmińsko-mazurskie","lubuskie","opolskie","podlaskie",
    "masovian","lesser","silesia","pomeranian","greater","lodz","subcarpathia","subcarpathian",
    "lower","opole","lubusz","podlachia","warmian","kujavian","west","holy"
]
region_regex = "(" + "|".join(region_words) + ")"

df = (
    df
    .withColumn("location_clean", F.lower(F.col("location")))
    .withColumn("location_clean", F.regexp_replace("location_clean", "siedziba firmy:|company location:", ""))
    .withColumn("location_clean", F.regexp_replace("location_clean", "\\(", ","))
    .withColumn("location_clean", F.regexp_replace("location_clean", "\\)", ""))
    .withColumn("location_clean", F.regexp_replace("location_clean", "\\s+", " "))
    .withColumn("location_clean", F.trim("location_clean"))
    .withColumn("city_from_whitelist", F.regexp_extract("location_clean", city_regex, 1))
    .withColumn("location_city_raw", F.regexp_extract("location_clean", r"([^,]+)$", 1))
    .withColumn("location_city_raw", F.trim(F.regexp_replace("location_city_raw", r"\s*\(.*\)", "")))
    .withColumn("fallback_city", F.initcap(F.regexp_extract("location_city_raw", r"([a-ząćęłńóśźż]+)", 1)))
    .withColumn(
        "location_city",
        F.when(F.col("city_from_whitelist") != "", F.initcap(F.col("city_from_whitelist")))
         .when(~F.col("fallback_city").rlike(region_regex), F.col("fallback_city"))
         .otherwise(F.lit(None))
    )
    .withColumn(
        "location_city",
        F.when(
            F.col("location_city").rlike(
                "(Mazowieckie|Małopolskie|Dolnośląskie|Śląskie|Pomorskie|Wielkopolskie|Łódzkie|Podkarpackie|Lubelskie|Kujawsko|Zachodniopomorskie|Świętokrzyskie|Warmińsko|Lubuskie|Opolskie|Podlaskie)"
            ),
            None
        ).otherwise(F.col("location_city"))
    )
    .withColumn(
        "location_city",
        F.when(F.lower(F.col("location_city")).rlike("(skie$|lia$)"), None)
         .otherwise(F.col("location_city"))
    )
    .withColumn("location_region_raw", F.regexp_extract("location_clean", r",\s*([a-ząćęłńóśźż\s]+)$", 1))
    .withColumn("location_region_raw", F.trim(F.col("location_region_raw")))
    .withColumn("location_region", F.coalesce(region_expr[F.col("location_region_raw")], F.col("location_region_raw")))
    .drop("location_clean", "city_from_whitelist", "location_city_raw", "fallback_city", "location_region_raw")
)

from pyspark.sql import functions as F

# --- mapowanie specjalizacji ---
spec_map = {
    "backend": "backend",
    "frontend": "frontend",
    "full-stack": "fullstack",
    "fullstack": "fullstack",
    "mobile": "mobile",
    "qa": "qa",
    "testing": "qa",
    "test": "qa",
    "devops": "devops",
    "architecture": "architecture",
    "architect": "architecture",
    "security": "security",
    "ai/ml": "data",
    "ai": "data",
    "ml": "data",
    "big data": "data",
    "data science": "data",
    "data analytics": "data",
    "bi": "data",
    "analytics": "data",
    "system analytics": "business_analysis",
    "business analytics": "business_analysis",
    "project management": "project_management",
    "product management": "project_management",
    "agile": "project_management",
    "scrum": "project_management",
    "sap": "sap_erp",
    "erp": "sap_erp",
    "helpdesk": "helpdesk_admin",
    "it admin": "helpdesk_admin",
    "support": "helpdesk_admin",
    "embedded": "embedded",
    "ux/ui": "ux_ui",
    "ux": "ux_ui",
    "ui": "ux_ui",
    "design": "ux_ui"
}

keys_sql = "(" + ", ".join([f"'{k}'" for k in spec_map.keys()]) + ")"
values_sql = "(" + ", ".join([f"'{v}'" for v in spec_map.values()]) + ")"

# --- tylko wyznaczenie specialization_general ---
df = (
    df
    .withColumn("spec_joined", F.when(F.col("specialization").isNotNull(), F.concat_ws(", ", F.col("specialization"))))
    .withColumn("spec_clean", F.lower(F.col("spec_joined")))
    .withColumn("spec_clean", F.regexp_replace("spec_clean", r"[\[\]]", ""))
    .withColumn("spec_clean", F.regexp_replace("spec_clean", r"&", ","))
    .withColumn("spec_clean", F.regexp_replace("spec_clean", r"/", ","))
    .withColumn("spec_clean", F.regexp_replace("spec_clean", r"\s+", " "))
    .withColumn("spec_clean", F.trim("spec_clean"))
    .withColumn("spec_array_raw", F.split("spec_clean", r"\s*,\s*"))
    # tylko pierwsza wartość mapowania — bez tworzenia array
    .withColumn(
        "specialization_general",
        F.element_at(
            F.expr(f"""
                filter(
                    transform(
                        spec_array_raw,
                        x -> map_from_arrays(array{keys_sql}, array{values_sql})[x]
                    ),
                    x -> x is not null
                )
            """),
            1
        )
    )
    .drop("spec_joined", "spec_clean", "spec_array_raw", "specialization", 
          "salary_raw", "salary_unit_raw", "valid_until_raw", "location")
)

# --- Dodanie scrap_date ---
df = df.withColumn("scrap_date", F.lit(TODAY).cast("date"))

# --- Uporządkowanie kolejności kolumn ---
cols = ["offer_id"] + [c for c in df.columns if c != "offer_id"]

df = df.select(*cols)

# === 7️⃣ Zapis ===
(
    df.write
      .mode("overwrite")
      .format("parquet")
      .option("compression", "snappy")
      .save(OUTPUT_PATH)
)

print("✅ Dane zapisane pomyślnie do GCS:")
print(OUTPUT_PATH)
spark.stop()
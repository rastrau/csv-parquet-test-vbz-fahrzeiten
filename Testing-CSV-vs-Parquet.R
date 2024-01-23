library(tictoc)
library(readr)
library(arrow)

url <- "https://data.stadt-zuerich.ch/dataset/vbz_fahrzeiten_ogd_2022/download/Fahrzeiten_SOLL_IST_20221225_20221231.csv"
filename <- "fahrzeiten_soll_ist_20221225_20221231.csv"

# Initally download the file from opendata.swiss for a fair comparison (no caching...)
download.file(url = url, destfile = filename)

# Load CSV
tic("Loading CSV without column specifications")
df <- read_delim(in_file)
toc()

# Load CSV while indicating attributes of type 'date'
tic("Loading CSV with column specifications for 'date' attributes")
df2 <- read_delim(
  in_file,
  col_types = cols(
    betriebsdatum = col_date(format = "%d.%m.%y"),
    datum_von = col_date(format = "%d.%m.%y"),
    datum_nach = col_date(format = "%d.%m.%y")
  )
)
toc()

# Load CSV while indicating all attribute types
tic("Loading CSV with column specifications for all attributes")
df3 <- read_delim(
  in_file,
  col_types = cols(
    linie = col_integer(),
    richtung = col_integer(),
    betriebsdatum = col_date(format = "%d.%m.%y"),
    fahrzeug = col_integer(),
    kurs = col_integer(),
    seq_von = col_integer(),
    halt_diva_von = col_integer(),
    halt_punkt_diva_von = col_integer(),
    halt_kurz_von1 = col_character(),
    datum_von = col_date(format = "%d.%m.%y"),
    soll_an_von = col_integer(),
    ist_an_von = col_integer(),
    soll_ab_von = col_integer(),
    ist_ab_von = col_integer(),
    seq_nach = col_integer(),
    halt_diva_nach = col_integer(),
    halt_punkt_diva_nach = col_integer(),
    halt_kurz_nach1 = col_character(),
    datum_nach = col_date(format = "%d.%m.%y"),
    soll_an_nach = col_integer(),
    ist_an_nach1 = col_integer(),
    soll_ab_nach = col_integer(),
    ist_ab_nach = col_integer(),
    fahrt_id = col_integer(),
    fahrweg_id = col_integer(),
    fw_no = col_integer(),
    fw_typ = col_integer(),
    fw_kurz = col_integer(),
    fw_lang = col_character(),
    umlauf_von = col_integer(),
    halt_id_von = col_integer(),
    halt_id_nach = col_integer(),
    halt_punkt_id_von = col_integer(),
    halt_punkt_id_nach = col_integer()
  )
)
toc()

# Look at size of objects (dataframes) in memory
object.size(df)
object.size(df2)
object.size(df3)

# Write dataframes to Parquet files
write_parquet(df, "parquet-test--df.parquet")
write_parquet(df2, "parquet-test--df2.parquet")
write_parquet(df3, "parquet-test--df3.parquet")

# Load data from Parquet file
tic("Loading dataframe from Parquet")
df3_ <- read_parquet("parquet-test--df3.parquet")
toc()

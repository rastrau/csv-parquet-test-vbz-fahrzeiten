library(microbenchmark)
library(readr)
library(arrow)

url <- "https://data.stadt-zuerich.ch/dataset/vbz_fahrzeiten_ogd_2022/download/Fahrzeiten_SOLL_IST_20221225_20221231.csv"
filename <- "fahrzeiten_soll_ist_20221225_20221231.csv"

# Initally download the file from opendata.swiss for a fair comparison (no caching...)
download.file(url = url, destfile = filename)

# Define functions for reading CSV file

# Load CSV normally
read_csv_without_specifications <- function(){
  df <- read_csv(filename)
  return(df)
}

read_csv_specifications_using_all <- function(){
  df <- read_csv(filename, guess_max = Inf)
  return(df)
}

# Load CSV while indicating attributes of type 'date'
read_csv_with_date_specifications <- function(){
  df <- read_csv(filename,
                  col_types = cols(
                    betriebsdatum = col_date(format = "%d.%m.%y"),
                    datum_von = col_date(format = "%d.%m.%y"),
                    datum_nach = col_date(format = "%d.%m.%y")
                    )
                  )
  return(df)
}

# Load CSV while indicating all attribute types
read_csv_with_all_specifications <- function(){
  df <- read_csv(filename,
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
  return(df)
}

# Benchmark functions
reading_csv = microbenchmark(
  'Read CSV w/o specs' = read_csv_without_specifications(),
  'Read CSV w/ date specs' = read_csv_with_date_specifications(),
  'Read CSV w/ all specs' = read_csv_with_all_specifications(),
  times = 10)

reading_csv
microbenchmark:::boxplot.microbenchmark(reading_csv)

reading_csv2 = microbenchmark(
  'Read CSV w/ specs based on all' = read_csv_specifications_using_all(),
  'Read CSV w/o specs' = read_csv_without_specifications(),
  times = 10)

reading_csv2
microbenchmark:::boxplot.microbenchmark(reading_csv2)


# Look at size of objects (dataframes) in memory
df = read_csv_without_specifications()
df2 = read_csv_with_date_specifications()
df3 = read_csv_with_all_specifications()
object.size(df)
object.size(df2)
object.size(df3)

# Write dataframes to Parquet files
write_parquet(df, "parquet-test--df.parquet")
write_parquet(df2, "parquet-test--df2.parquet")
write_parquet(df3, "parquet-test--df3.parquet")

# Loading data from Parquet files
df_ <- read_parquet("parquet-test--df.parquet")
df2_ <- read_parquet("parquet-test--df2.parquet")
df3_ <- read_parquet("parquet-test--df3.parquet")

reading_parquet = microbenchmark(
  'Read Parquet w/o specs' = read_parquet("parquet-test--df.parquet"),
  'Read Parquet w/ date specs' = read_parquet("parquet-test--df2.parquet"),
  'Read Parquet w/ all specs' = read_parquet("parquet-test--df3.parquet"),
  times = 10)

reading_parquet
microbenchmark:::boxplot.microbenchmark(reading_parquet)

reading_csv_and_parquet = microbenchmark(
  'Read CSV w/o specs' = read_csv_without_specifications(),
  'Read Parquet w/ all specs' = read_parquet("parquet-test--df3.parquet"),
  times = 10)

reading_csv_and_parquet
microbenchmark:::boxplot.microbenchmark(reading_csv_and_parquet)

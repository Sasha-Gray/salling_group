#
# Salling Group Food Waste Day 2 #
#

# Her henter vi kun al det nye data ned fra API'et for produkt/tilbud.
# Ikke for butikker, da vi ikke regner med at der kommer nye butikker.

# Trin 0 - Indlæs relevante pakker:
library(httr)
library(jsonlite)
library(dplyr)
library(DBI)
library(RMariaDB)

# Trin 1 - Indsæt API-token:
api_token <- "SG_APIM_MK4NSVQMX61WWNDRTCD980PXR6NMEVE4N420CPXF89W8YNVF2CP0"

# Trin 2 - Vælg 4 postnumre:
zip_codes <- c("3400", "3200", "2840", "2400")

# Trin 3 - Lav en liste til at gemme resultaterne i
all_data <- data.frame()

# Trin 4 - Lav et for-loop der henter alle dataene ned:
# Definér først timestamp for hele denne kørsel
run_timestamp <- Sys.time()

for (z in zip_codes) {
  
  # Hent data
  url <- paste0("https://api.sallinggroup.com/v1/food-waste/?zip=", z)
  
  res <- GET(url, add_headers(Authorization = paste("Bearer", api_token)))
  
  # Tjek statuskode
  if (status_code(res) != 200) {
    warning("Fejl. Status: ", status_code(res))
    next  # spring videre til næste postnummer
  }
  
  # Hent indhold som tekst og parse JSON
  txt <- content(res, as = "text", encoding = "UTF-8")
  dat <- fromJSON(txt, flatten = TRUE)
  
  # Hvis ingen butikker
  if (nrow(dat) == 0) next
  
  # Pak alle butikkers clearances ud automatisk
  clear <- bind_rows(dat$clearances)
  
  # Tilføj stamdata
  clear$store_id   <- rep(dat$store.id,   times = sapply(dat$clearances, nrow))
  clear$store_name <- rep(dat$store.name, times = sapply(dat$clearances, nrow))
  clear$zip        <- z
  clear$run_timestamp <- run_timestamp
  
  # Tilføj til samlet tabel
  all_data <- bind_rows(all_data, clear)
}

print(nrow(all_data))
print(names(all_data))

# Nu kan vi se hvilke data og tabeller vi kan lave og vil have med over i MySQL.
# Så nu går vi over i MySQL Workdbench og laver de relevante tabeller.

#
# HERFRA overfører vi R-scriptet til GitHub og henter det ned på Ubuntu-serveren,
# og kører hele R-scriptet på Ubuntu-serveren, hvor den så connecter til MySQL og indsætter dataen
#

# Nu er vi klar til at udfylde de to tabeller vi oprettede i MySQL, med vores data:
# Trin 5 - Opret connection til MySQL:
con <- dbConnect(
  MariaDB(),
  user = "sallinguser",
  password = "SallingUser2025!",
  host = "localhost",
  port = 3306,
  dbname = "sallingdb")

# Trin 6 - Udvælg de kolonner der er relevante at tage med over i MySQL, omhandlende produktere og tilbuddende:
discount <- all_data %>%
  transmute(
    store_id = store_id,
    ean = product.ean,
    description = product.description,
    new_price = offer.newPrice,
    original_price = offer.originalPrice,
    discount_percent = offer.percentDiscount,
    stock = offer.stock,
    stock_unit = offer.stockUnit,
    run_timestamp = run_timestamp)

# Trin 6.1 - Udfyld tabellen med produkterne og tilbuddende i MySQL med ovenstående data:
dbWriteTable(con, "discount_products", discount, append = TRUE, row.names = FALSE)

# Trin 7 - Laver man en logfil:
write(paste(Sys.time(), "- hentet", nrow(all_data), "tilbud"), 
      file = "foodwaste_log2.txt", 
      append = TRUE)

# Trin 8 - Man skal huske at disconnecte fra databasen og det gøre man ved:
dbDisconnect(con)

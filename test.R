invisible(x = {
  require(read.dbc)
  require(data.table)
  require(dplyr)
  require(purrr)
  require(rvest)
  require(httr)
  require(fs)
  require(usethis)
  require(yaml)
  require(DBI)
  require(RPostgres)
})


fs::dir_ls(path = './src/',type = 'file') %>% 
  purrr::walk(~source(.,encoding = 'UTF-8'))


conn = fun_create_conn(
  user = credentials$user,
  pass = credentials$pass,
  dataset = credentials$dataset,
  host = credentials$host,
  port = credentials$port
)


DBI::dbGetQuery(conn, 'select distinct estado from datasus_sia')

DBI::dbGetQuery(conn, 'select count(*) as nrow from datasus_sia')

DBI::dbDisconnect(conn)
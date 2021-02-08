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

estados = fun_get_estados()

year_month = seq.Date(
  from = as.Date('2008-01-01'),
  to = Sys.Date(),
  by = 'day'
) %>% 
  format('%y%m')


if(!fs::dir_exists('./data/sia_data/')){
  fs::dir_create('./data/sia_data')
}


# database
credentials = yaml::read_yaml('~/umane_credentials.yml')

conn = fun_create_conn(
  user = credentials$user,
  pass = credentials$pass,
  dataset = credentials$dataset,
  host = credentials$host,
  port = credentials$port
)

new = TRUE

if(new){
  query_remove_table <- 'DROP TABLE IF EXISTS  datasus_sia '
  DBI::dbSendQuery(conn, query_remove_table)
  
  
  query_create_table <- '
  CREATE TABLE datasus_sia(
      id  SERIAL,
      cod_uf VARCHAR(2),
      cod_municipio VARCHAR(6),
      proc_id VARCHAR(50),
      sexo VARCHAR(1),
      faixa_etaria VARCHAR(50),
      racacor VARCHAR(50),
      valor INT
  );
'
  DBI::dbSendQuery(conn, query_create_table)
  
}


gc()


files = fun_sia_links()

purrr::walk(
  .x = files[1],
  .f = ~{
    url_base = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/'
    final_url = glue::glue('{url_base}{.x}')
  
    usethis::ui_todo(
      paste('Getting',crayon::bold(crayon::green(final_url)),   sep = ' > ')
    )
    name = stringr::str_remove(.x, '.dbc') %>% stringr::str_squish()
    date = stringr::str_extract(.x, '\\d+') %>% 
      stringr::str_c('01') %>% 
      lubridate::ymd()
    
    save_fn =  glue::glue('./data/sia_data/{name}.rds')
    
    
    try(
      expr = {
    
        dest_file = paste0(tempfile(),'.dbc')
        download.file(final_url,dest_file)
    
        dados = fun_sia_read_dbc(dest_file,date) %>% 
        fun_sia_prep() #%>% 
        #saveRDS(save_fn, compress = 'bzip2') 

        file.remove(dest_file)
        
        #save
        usethis::ui_todo(
          paste('Saving',crayon::bold(crayon::green(.x)),   sep = ' > ')
        )
        tryCatch({
          DBI::dbWriteTable(
            conn, 
            name = 'datasus_sia',
            value = dados,
            append = TRUE
          )  
        }, 
        error = function(e){
          conn = fun_create_conn(
            user = credentials$user,
            pass = credentials$pass,
            dataset = credentials$dataset,
            host = credentials$host,
            port = credentials$port
          )
          
          DBI::dbWriteTable(
            conn, 
            name = 'datasus_sia',
            value = dados,
            append = TRUE
          )
        })
        
        
    })
    gc()
  }
)



# save to db 




# save 

# files = fs::dir_ls(path = './data/sia_data/', type = 'file')
# 
# purrr::walk(
#   .x = files,
#   .f=~{
#     readRDS(.x) %>% 
#       DBI::dbWriteTable(
#        conn, 
#        name = 'datasus_sia',
#        value = .,
#        append = TRUE
#       )
#     gc()
#   }
#   )






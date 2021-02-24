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

'%out%' <- function(x,y){!(x%in%y)}

fs::dir_ls(path = './src/',type = 'file') %>% 
  purrr::walk(~source(.,encoding = 'UTF-8'))
gc()

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
credentials = tryCatch({
  yaml::read_yaml('~/umane_credentials.yml')
},
warning = function(e){
  yaml::read_yaml('~/Documents/umane_credentials.yml')
}
)  


conn = fun_create_conn(
  user = credentials$user,
  pass = credentials$pass,
  dataset = credentials$dataset,
  host = credentials$host,
  port = credentials$port
)

new = TRUE

if(new){
  query_remove_table <- 'DROP TABLE IF EXISTS  datasus_sih_internacoes,datasus_sih_dias,datasus_sih_gastos'
  DBI::dbSendQuery(conn, query_remove_table)
  
  # DBI::dbFetch(res)
  
  
  query_create_table1 <- '
  CREATE TABLE datasus_sih_internacoes(
      id  SERIAL,
      estado varchar(2),
      cod_uf VARCHAR(2),
      cod_municipio VARCHAR(6),
      tema VARCHAR(70),
      cids VARCHAR(50),
      sexo VARCHAR(10),
      faixa_etaria VARCHAR(50),
      racacor VARCHAR(50),
      obito VARCHAR(10),
      valor INT,
      ano INT,
      mes INT
  );
'
  DBI::dbSendQuery(conn, query_create_table1)
   
  query_create_table2 <- '
  CREATE TABLE datasus_sih_dias(
      id  SERIAL,
      estado varchar(2),
      cod_uf VARCHAR(2),
      cod_municipio VARCHAR(6),
      tema VARCHAR(70),
      cids VARCHAR(50),
      sexo VARCHAR(10),
      faixa_etaria VARCHAR(50),
      racacor VARCHAR(50),
      valor FLOAT,
      ano INT,
      mes INT
  );
'
  DBI::dbSendQuery(conn, query_create_table2)
  
  query_create_table3 <- '
  CREATE TABLE datasus_sih_gastos(
     id  SERIAL,
      estado varchar(2),
      cod_uf VARCHAR(2),
      cod_municipio VARCHAR(6),
      tema VARCHAR(70),
      cids VARCHAR(50),
      sexo VARCHAR(10),
      faixa_etaria VARCHAR(50),
      racacor VARCHAR(50),
      valor_tipo VARCHAR(20),
      valor_total FLOAT,
      valor_sh FLOAT,
      valor_sp FLOAT,
      ano INT,
      mes INT
  );
'
  
  DBI::dbSendQuery(conn, query_create_table3)
  
  
  
}


gc()


files = fun_sih_links()




#uf_in_db = DBI::dbGetQuery(conn, 'select distinct estado from datasus_sih_gastos')


# 
# estados = estados$estado_sigla[
#   estados$estado_sigla %out% uf_in_db$estado
# ]


purrr::walk(
  .x = estados$estado_sigla,
  .f = ~{
    states_files = files[
      stringr::str_detect(files,paste0(.x,'\\d{4}'))
    ]
    
    
    fs::dir_create('./data/sih_dbc/')
    
    purrr::walk(
      .x = states_files,
      .f=~{
        
        url_base = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/'
        final_url = glue::glue('{url_base}{.x}')
        
        usethis::ui_todo(
          paste('Getting', Sys.time(),crayon::bold(crayon::green(.x)),   sep = ' > ')
        )
        
        dest_file = glue::glue('./data/sih_dbc/{.x}')
        
        name = stringr::str_remove(.x, '.dbc') %>% stringr::str_squish()
        date = stringr::str_extract(.x, '\\d+') %>% 
          stringr::str_c('01') %>% 
          lubridate::ymd()
        
        #save_fn =  glue::glue('./data/sia_data/{name}.rds')
        
        tryCatch(
          {
            download.file(final_url,dest_file,quiet = TRUE) 
          },
          error = function(e){
            fun_save_log(.x,type = 'error', message = '', block= 'download')
          },
          waring = function(w){
            fun_save_log(.x,type = 'warning', message = '',block= 'download')
          }
        ) # fim downlod tryCatch 
        
        estado = stringr::str_remove(.x, '.dbc') %>% 
          stringr::str_remove('^RD') %>% 
          stringr::str_remove('a|b|c') %>% 
          stringr::str_remove("\\d+") %>% 
          stringr::str_squish()
        
        invisible({
          dados = tryCatch(
          {
             read.dbc::read.dbc(dest_file) %>% 
              fun_sih_prep() 
          },
          error = function(e){
            print(e)
            fun_save_log(
              .x,
              type = 'error', 
              message = '', 
              block= 'prep_data'
            )
            NULL
          },
          warning = function(w){
            print(w)
            fun_save_log(
              .x,
              type = 'warning', 
              message ='',
              block= 'prep_data'
            )
            NULL
          },finally = {
            NULL
          }
        ) # fim prep tryCatch
        })#fim invisible
        
        
        try(fs::file_delete(dest_file))
        
        gc()
        
        if( !is.null(dados$datasus_sih_internacoes) ){
          tryCatch({
             DBI::dbWriteTable(
              conn, 
              name = 'datasus_sih_dias',
              value = dados$datasus_sih_dias,
              append = TRUE
            )
            
            DBI::dbWriteTable(
              conn, 
              name = 'datasus_sih_internacoes',
              value = dados$datasus_sih_internacoes,
              append = TRUE
            )
            DBI::dbWriteTable(
              conn, 
              name = 'datasus_sih_gastos',
              value = dados$datasus_sih_gastos,
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
              name = 'datasus_sih_dias',
              value = dados$datasus_sih_dias,
              append = TRUE
            )
           DBI::dbWriteTable(
             conn, 
             name = 'datasus_sih_internacoes',
             value = dados$datasus_sih_internacoes,
             append = TRUE
           )
           DBI::dbWriteTable(
             conn, 
             name = 'datasus_sih_gastos',
             value = dados$datasus_sih_gastos,
             append = TRUE
           )
            
            
          }) # fim save db tryCatch 
        }
        
        
      }
    )
    fs::dir_delete('./data/sih_dbc/')
    gc()
  }
)


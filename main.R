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
  query_remove_table <- 'DROP TABLE IF EXISTS  datasus_sia '
  DBI::dbSendQuery(conn, query_remove_table)
 
 # DBI::dbFetch(res)
  
  
  query_create_table <- '
  CREATE TABLE datasus_sia(
      id  SERIAL,
      cod_uf VARCHAR(2),
      cod_municipio VARCHAR(6),
      proc_id VARCHAR(50),
      sexo VARCHAR(1),
      faixa_etaria VARCHAR(50),
      racacor VARCHAR(50),
      valor INT,
      ano INT,
      mes INT
  );
'
  DBI::dbSendQuery(conn, query_create_table)
  
  #DBI::dbFetch(res)
  
}


gc()


files = fun_sia_links()




purrr::walk(
  .x = estados$estado_sigla,
  .f = ~{
    states_files = files[
      stringr::str_detect(files,paste0(.x,'\\d{4}'))
    ]
    
    
      fs::dir_create('./data/sia_dbc/')
    
    
    purrr::walk(
      .x = states_files,
      .f=~{
        
        url_base = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/'
        final_url = glue::glue('{url_base}{.x}')
        
        usethis::ui_todo(
          paste('Getting', Sys.time(),crayon::bold(crayon::green(.x)),   sep = ' > ')
        )
        
        dest_file = glue::glue('./data/sia_dbc/{.x}')
        
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
            fun_save_log(.x,type = 'error', message = e, block= 'download')
          },
          waring = function(w){
            fun_save_log(.x,type = 'warning', message = w,block= 'download')
          }
        ) # fim downlod tryCatch 
        
        
        dados = tryCatch(
          {
            fun_sia_read_dbc(dest_file,date) %>% 
            fun_sia_prep() 
          },
          error = function(e){
            print(e)
            fun_save_log(
              .x,
              type = 'error', 
              message = as.character(e$message), 
              block= 'prep_data'
              )
            NULL
          },
          warning = function(w){
            print(w)
            fun_save_log(
              .x,
              type = 'warning', 
              message =as.character(w$message),
              block= 'prep_data'
            )
            NULL
          },finally = {
            NULL
          }
        ) # fim prep tryCatch
        
        try(fs::file_delete(dest_file))
        gc()
        if( !is.null(dados) ){
          tryCatch({
            res = DBI::dbWriteTable(
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
            res = DBI::dbWriteTable(
              conn, 
              name = 'datasus_sia',
              value = dados,
              append = TRUE
            )
            
            
          }) # fim save db tryCatch 
        }
        
        
      }
    )
    fs::dir_delete('./data/sia_dbc/')
    gc()
  }
)

require(read.dbc)
require(data.table)
require(dplyr)
require(purrr)
require(rvest)
require(httr)
require(fs)
require(usethis)


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


links = fun_sia_links()

purrr::walk(
  .x = files,
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
    
    dest_file = paste0(tempfile(),'.dbc')
    download.file(final_url,dest_file)
    fun_sia_read_dbc(dest_file,date) %>% 
    fun_sia_prep() %>% 
    saveRDS(save_fn, compress = 'bzip2') 
    
    file.remove(dest_file)
    gc()
  }
)










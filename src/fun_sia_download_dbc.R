fun_sin_download_dbc <- function(date, estado){
  
  url_base = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/'
  usethis::ui_line(
    paste(
      '    date',
      crayon::bold(crayon::green(date))      
    ,   sep = ' > '))
    
    
  year_month = date
  url <- glue::glue('{url_base}PA{estado}{year_month}.dbc')
  save_fn =  glue::glue('./data/sia_file/{estado}{date}.rds')
   tryCatch({
      dest_file = paste0(tempfile(),'.dbc')
      download.file(url,dest_file)
      dados = fun_sia_read_dbc(dest_file)
      file.remove(dest_file)
      dados
    },
    error = function(e){
    
      files = c()
      i = 1
      # for para obter arquivos particionados
      for(letter in letters[1:3]){
        dest_file = paste0(tempfile(),'.dbc')
        url <- glue::glue('{url_base}PA{estado}{year_month}{letter}.dbc')
        tryCatch({
          download.file(url,dest_file)
          files[i] = dest_file
          i = i + 1
        },
        error = function(e){
          next
        })
        
        dados = purrr::map_dfr(files,~fun_sia_read_dbc(.x))
        file.remove(files)
        dados
      }  
    },)

    
  # if(exists('files'))  {
  #   dados = purrr::map_dfr(files,~fun_sia_read_dbc(.x))
  #   file.remove(files)
  # }else{
  #   dados = fun_sia_read_dbc(dest_file)
  #   file.remove(dest_file)
  # }    
  # 
  fun_sia_prep(dados) %>% 
  saveRDS(save_fn, compress = 'bzip2')
  
}

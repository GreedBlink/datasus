fun_save_log <- function(x,type,message,block){
  if(!fs::dir_exists('./data/log')){
    fs::dir_create('./data/log/')
  }
  
  if(fs::file_exists('./data/log/log_files.rds')){
    readRDS('./data/log/log_files.rds') %>% 
      dplyr::bind_rows(
        tibble::tibble(
          datetime = Sys.time(),
          file = x,
          type = type,
          message = message,
          block = block
        )
      ) %>% 
      saveRDS('./data/log/log_files.rds',compress = 'bzip2')
  }else{
    tibble::tibble(
      datetime = Sys.time(),
      file = x,
      type = type,
      message = message,
      block = block
      
    ) %>% 
      saveRDS('./data/log/log_files.rds',compress = 'bzip2')
  }

  return(message('log saved!'))
}
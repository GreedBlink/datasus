fun_check_states <- function(state,files_param, conn){
  
  dates = seq.Date(
    from = as.Date('2008-01-01'),
    to = as.Date('2020-12-01'),
    by = 'month'
  )
  
  DBI::dbListFields(conn, 'datasus_sia')
  db_data = DBI::dbGetQuery(
    conn,
    glue::glue(
      "SELECT estado,ano, mes FROM datasus_sia WHERE estado like '{state}'"
    )
  ) %>% 
    tibble::as_tibble() %>% 
    dplyr::mutate(
      date = glue::glue('{ano}-{mes}-{01}') %>% lubridate::ymd() %>% format('%y%m')
    ) %>% dplyr::distinct(date)
  dates  = dates %>% format('%y%m')
  dates = dates[dates %out% db_data$date]
  
  files_out = purrr::map(
    .x = dates,
    .f=~{
      files_param[stringr::str_detect(files_param,paste0(state,.x))]
   }) %>%  unlist()
  
  
  
  if(length(files_out) != 0){
   return(files_out)
  }else{
    return(files)
  }
  
  
}
fun_sia_read_dbc <- function(file,date){
  
  ano = lubridate::year(date)
  mes = lubridate::month(date)
  
  procs = c('0211040029', '0204030030', '0205020097', '0209010029')
  read.dbc::read.dbc(file) %>% 
    data.table::as.data.table() %>% 
    .[,PA_PROC_ID:= as.character(PA_PROC_ID)] %>% 
    .[,':='(ano = ano,mes = mes)] %>% 
    .[PA_PROC_ID %chin% procs] 
}
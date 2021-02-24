fun_get_estados = function(){
 estado_siglas =  GET(
    'https://brasilescola.uol.com.br/brasil/estados-brasil.htm'
  ) %>% 
    httr::content() %>% 
    rvest::html_node('div.table-responsive > table') %>% 
    rvest::html_table() %>% 
    .[-1,] %>% 
    purrr::set_names('estado_name','capitais','estado_sigla') %>% 
    tibble::as_tibble()
  
 # estado_codes =  GET(
 #   'https://www.consultaddd.com/'
 # ) %>% 
 #   httr::content() %>% 
 #   rvest::html_node('div#divConteudo > table.tablesorter') %>% 
 #   rvest::html_table() %>% 
 #   .[,-1] %>% 
 #   purrr::set_names('estado_name','capitais','estado_code') %>% 
 #   tibble::as_tibble()
 # 
 estado_siglas$estado_name = estado_siglas$estado_name %>% 
   stringr::str_remove("\\*") %>% 
   stringr::str_squish()
 
 # output = estado_siglas %>% 
 #   dplyr::left_join(
 #     estado_codes %>% dplyr::select(estado_name,estado_code),
 #     by = 'estado_name'
 #   )
 # 
 
 estado_siglas
 
}


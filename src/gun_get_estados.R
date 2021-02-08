fun_get_estados = function(){
  GET(
    'https://brasilescola.uol.com.br/brasil/estados-brasil.htm'
  ) %>% 
    httr::content() %>% 
    rvest::html_node('div.table-responsive > table') %>% 
    rvest::html_table() %>% 
    .[-1,] %>% 
    purrr::set_names('estado_name','capitais','estado_sigla') %>% 
    tibble::as_tibble()
  
}
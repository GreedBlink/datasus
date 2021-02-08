
fun_sia_links <- function(){
  library(rvest)
  library(httr)
  library(xml2)
  library(stringr)
  
  page <- xml2::read_html("ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/")
  
  table <- page %>%
    rvest::html_node("body p") %>% 
    rvest::html_text() %>%
    stringr::str_split("\n") %>% unlist() %>% 
    stringr::str_split(" ") %>% unlist()
  
  condition <- table %>% stringr::str_detect(".dbc")
  dbcs <- table[condition] 
  
  condition2 <- dbcs %>% stringr::str_detect("\\bPA")
  
  dbcs[condition2] 
  
}
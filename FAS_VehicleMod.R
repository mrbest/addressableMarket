require(readr)
require(dplyr)


main <- function()
{
  load_files()
  vehicle_list <<- generate_vehicle_df_list(Contract_Inventory, transactions)
  psc_naics_proportions_list <<- produce_psc_naics_proportions(vehicle_list, Contract_Inventory)
  market_list <<- generate_vehicle_market_list(psc_naics_proportions_list, Contract_Inventory, transactions)
  market_df <<- assemble_market_df(market_list)
  write_csv(market_df, "market_df.csv")
}



load_files <- function()
{##future dev note: file loading phase can be speeded up by using spark csv reader
  options(scipen = 999)
  #Read in file
  df <- read_csv("GWCM_FPDS_01_MAY_EXTRACT.csv")
  #filter by date range to only have FY16
  transactions <- df %>% filter(as.Date(date_signed) >= as.Date("2015-10-01") & as.Date(date_signed) <= as.Date("2016-09-30"))
  transactions <<- transactions %>% mutate(psc_naics_combo =paste0(product_or_service_code, "_", naics_code))
  #clear up RAM
  gc()
  Contract_Inventory <<- read_csv("2017_06_20_Contract_Inventory_All_Fields.csv")
}

generate_vehicle_df_list <- function(contract_inventory, transaction_df)
{
  #id GSA contract solutions from contract inventory
  gsa_vehicle_df <- contract_inventory %>% filter(managing_agency == "GSA")
  #create list of GSA contract names to iterate through
  unique_gsa_vehicleNames <- gsa_vehicle_df %>% select(contract_name) %>% distinct()
  #initialize vehicle dataframe list
  vehicle_df_list <- list()
  #create dataframe for each vehicle and append to list
  for(i in 1:nrow(unique_gsa_vehicleNames))
  {
   current_vehicle_ref_piids <- contract_inventory %>% filter(contract_name %in% unique_gsa_vehicleNames[i, 1]) %>% select(unique_contract_id)
   ##data management recommendation: change unique_contract_id to reference_piid or add new column called reference_piid with same values CONSISTENCY!
   current_vehicle_df <- transaction_df %>% filter(reference_piid %in% current_vehicle_ref_piids$unique_contract_id) 
   ##here it may be useful to know of any contract vehicles that do not yield transactions to prevent empty data frames upstream 
   
   if(nrow(current_vehicle_df) >0 & is.null(nrow(current_vehicle_df)) == FALSE)
     {vehicle_df_list[[i]] <- current_vehicle_df}   
  }
  #return list of vehicle data frames
  vehicle_df_list
}


produce_psc_naics_proportions <- function(contract_vehicle_list, contract_inventory)
{
  psc_naics_proportions_list <- list()
  vehicle_list_count <- length(contract_vehicle_list)
  for(i in 1:vehicle_list_count) #loop through vehicles
  {  
    
  contract_vehicle_df <- contract_vehicle_list[[i]] 
  contract_vehicle_df_count <- nrow(contract_vehicle_df)
  if(is.null(contract_vehicle_df_count) == FALSE)
  {
    contract_vehicle_psc_naics_combos <- produce_psc_naics_combos(contract_vehicle_df, contract_inventory) 
    psc_naics_proportions_list[[i]] <- contract_vehicle_psc_naics_combos
  } 
  else
  {#write empty table to  
    psc_naics_proportions_list[[i]] <- data.frame(product_or_service_code = NA, naics_code = NA, dollars_obligated = 0, vehicle_proportion = 0, contract_name = "None") 
  }  
  
  
  }##end of vehicle list loop
  psc_naics_proportions_list
}


produce_psc_naics_combos <- function(contract_vehicle_df, contract_inventory)
{
  vehicle_obligation_total <- sum(contract_vehicle_df$dollars_obligated)
  psc_naics_combos <- contract_vehicle_df %>% 
    filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE) %>% 
    select(product_or_service_code, naics_code) %>% 
    distinct(product_or_service_code, naics_code)
  #loop: calculate each combo's percentage of the whole vehicle 

  psc_naics_combo_sum_vector <- numeric()
  psc_naics_combo_proportion <- numeric()
  #get the vehicle name
  psc_naics_combo_contract_vehicle <- contract_inventory %>% filter(unique_contract_id %in% contract_vehicle_df$reference_piid) %>% select(contract_name) %>% distinct() %>% .$contract_name
  for(j in 1:nrow(psc_naics_combos)) #loop through psc_naics combos
  {
    
    current_psc_naics_combo <- psc_naics_combos[j, ]
    psc_naics_combo_transactions <- contract_vehicle_df %>% 
      filter(product_or_service_code == current_psc_naics_combo$product_or_service_code & naics_code == current_psc_naics_combo$naics_code)%>%
      select(product_or_service_code, naics_code, dollars_obligated)
    if(nrow(psc_naics_combo_transactions)>0){
      psc_naics_combo_obligation_total <- psc_naics_combo_transactions %>% select(dollars_obligated) %>% na.omit %>% sum()
      psc_naics_combo_sum_vector <- append( psc_naics_combo_sum_vector, psc_naics_combo_obligation_total)
      psc_naics_combo_proportion <- append(psc_naics_combo_proportion, psc_naics_combo_obligation_total/vehicle_obligation_total)} 
    else{psc_naics_combo_sum_vector <- append( psc_naics_combo_sum_vector, 0)
    psc_naics_combo_proportion <- append(psc_naics_combo_proportion, 0)
    }#end of else
  }#end of psc naics combo loop
  psc_naics_combos$dollars_obligated <- psc_naics_combo_sum_vector
  psc_naics_combos$vehicle_proportion <- psc_naics_combo_proportion
  psc_naics_combos$contract_name <- rep(psc_naics_combo_contract_vehicle, length(psc_naics_combo_proportion))
  #create equal length contract vehicle vector to 
  
  psc_naics_combos
}





produce_psc_naics_market <- function(vehicle_psc_naics_combo_df, contract_inventory, transaction_df)
{
  contract_vehicle_ref_piids <- contract_inventory %>% 
    filter(contract_name %in% vehicle_psc_naics_combo_df$contract_name) %>% 
    select(unique_contract_id)
  
  #prep_vehicle_psc_naics_combo_df 
  vehicle_psc_naics_combo_df <- vehicle_psc_naics_combo_df %>% mutate(psc_naics_combo = paste0(product_or_service_code, "_", naics_code))
  #get the market
  vehicle_market <- transaction_df %>% filter(psc_naics_combo %in% vehicle_psc_naics_combo_df$psc_naics_combo) 
  
  vehicle_market <- vehicle_market %>% filter(!reference_piid %in% contract_vehicle_ref_piids )
  vehicle_market
  
  #produces one market. market still neeeds summary info with contract info blended.
}

generate_vehicle_market_list <- function(psc_naics_proportions_list, contract_inventory, transaction_df)
{
  vehicle_market_list <- list()
  listCount <- length(psc_naics_proportions_list)
  for(i in 1:listCount)
  {
    current_vehicle_psc_naics_df <- psc_naics_proportions_list[[i]]
    current_vehicle_name <- current_vehicle_psc_naics_df %>% select(contract_name) %>% distinct() %>% .$contract_name
    current_vehicle_market_df <- produce_psc_naics_market(psc_naics_proportions_list[[i]], contract_inventory, transaction_df)
    current_vehicle_market_df$contract_perspective <- rep(current_vehicle_name, nrow(current_vehicle_market_df))
    vehicle_market_list[[i]] <- current_vehicle_market_df
  }
vehicle_market_list  
}

assemble_market_df <- function(vehicle_market_list)
{
  market_list_length <- length(vehicle_market_list)
  market_df <- vehicle_market_list[[1]]
  for(i in 2:market_list_length)
  {
    market_df <- rbind(market_df, vehicle_market_list[[i]])
  }  
 
 market_df 
}

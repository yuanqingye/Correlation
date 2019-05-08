cal_correlation_edited = function(data,user_col,mat_col){
  #data = fetch(res)
  data = data[nchar(str_trim(data[[user_col]]))!=0 & !is.na(data[[user_col]]) & !is.null(data[[user_col]]),]
  if(mat_col == "mobile"){
    data = data[mobile != "/" & mobile != "//" & mobile != "-" & !str_detect(mobile,"^12|^11|^1$") & mobile != "" & mobile != "",]}
  data = data[data[[mat_col]]!="",]
  user = unique(data[[user_col]]) 
  mat = unique(data[[mat_col]])
  uidx = match(data[[user_col]], user)
  midx = match(data[[mat_col]], mat)
  M = matrix(0, length(user), length(mat))
  i = cbind(uidx, midx)
  dti = data.table(i)
  colnames(dti) = c("uidx","midx")
  stat_dti = dti[,.(freq = .N),by = c("uidx","midx")]
  matrix_i = as.matrix(stat_dti[,c("uidx","midx")])
  M[matrix_i]= stat_dti$freq #this step can be modified
  mod = colSums(M^2)^0.5      
  MM = M %*% diag(1/mod)      
  S = crossprod(MM)
  rownames(S) = mat
  colnames(S) = mat
  return(S)
}

cal_correlation = function(data,user_col,mat_col){
  #data = fetch(res)
  user = unique(data[[user_col]]) 
  mat = unique(data[[mat_col]])
  uidx = match(data[[user_col]], user)
  midx = match(data[[mat_col]], mat)
  M = matrix(0, length(user), length(mat))
  i = cbind(uidx, mid)
  M[i]=1   
  mod = colSums(M^2)^0.5    
  MM = M %*% diag(1/mod)    
  S = crossprod(MM)     
}

get_ordered_correlation_table = function(data,N = 11){
  temp = as.data.frame(data)
  temp = as.data.table(temp)
  temp$id = rownames(data)
  melt_result = melt.data.table(temp,id.vars = "id",measure.vars = 1:(ncol(temp)-1))
  order_result = melt_result[order(id,value,decreasing = TRUE),]
  trunk_result = order_result[,.SD[2:N,],by = "id"]
  return(trunk_result)
}

get_order_data = function(){
  source("~/Rfile/R_hive.R") ##!!need to include that file
  jinqiao_sale_data_sql = "select ordr_date,prod_name,cont_main_brand_name,mall_name,shop_id,shop_name,contract_code,booth_id,booth_desc,partner_name,open_id,cust_id,act_amt,cust_name,mobile from dl.fct_ordr where act_amt > 0 and ordr_status not in ('1','7','19','Z','X') and mall_name = '上海金桥商场'"
  jinqiao_sale_data = read_data_hive_general(jinqiao_sale_data_sql)
  jinqiao_sale_data = data.table(jinqiao_sale_data)
  return(jinqiao_sale_data)
}

get_tracking_data = function(mon = "initial"){
  if(mon == "initial"){
    load("~/R_Projects/sale_model_standard/smart_mall_track_data_list_second.RData")
    load("~/R_Projects/sale_model_standard/smart_mall_list.RData")
    smart_mall_track_data_jan = rbindlist(smart_mall_track_data_list)
    smart_mall_track_data_feb = rbindlist(smart_mall_list)
    smart_mall_track_data = rbind(smart_mall_track_data_jan,smart_mall_track_data_feb)
  }
  else if (mon == "March"){
    load("~/R_Projects/Correlation/smart_mall_track_data_march.RData")
    smart_mall_track_data = rbindlist(smart_mall_track_data_list)
  }
  else{
    load(paste0("~/R_Projects/Correlation/smart_mall_",mon,"_list.RData"))
    smart_mall_track_data = rbindlist(eval(as.name(paste0("smart_mall_",mon,"_list"))))
  }
  #need to be filled
  return(smart_mall_track_data)
}

get_mall_correlation_result_using_order = function(type,refetch = FALSE){
  # 根据订单来找关联度
  if(!exists("jinqiao_sale_data")|refetch == TRUE){
    jinqiao_sale_data = get_order_data()
   }
  # data = jinqiao_sale_data
  if(type == "brand"){
    jinqiao_brand_correlation = cal_correlation_edited(jinqiao_sale_data,"mobile","cont_main_brand_name")
    result = get_ordered_correlation_table(jinqiao_brand_correlation)
    write.csv(result,"~/data/jinqiao_brand_correlation_order_melt.csv")
  }
  else{
    jinqiao_shop_correlation = cal_correlation_edited(jinqiao_sale_data,"mobile","shop_name")
    result = get_ordered_correlation_table(jinqiao_shop_correlation)
    write.csv(result,"~/data/jinqiao_shop_correlation_order_melt.csv")}
  return(result)
}


get_mall_correlation_result_using_track = function(smart_mall_track_data,type = "shop",notimelimit = TRUE,nothreshold = TRUE,threshold=2,startdate,enddate,store_brand_dict = parent.frame()$store_brand_dict)
{
  if(type == "shop"){
    smart_mall_track_data = smart_mall_track_data[event_type == 0,]
    # smart_mall_track_data$time_duration = difftime(smart_mall_track_data$exit_time,smart_mall_track_data$enter_time,units = "mins")
    if(notimelimit&nothreshold){
      smart_mall_track_data_oneday_atime = smart_mall_track_data[,.(freq = .N),by = c("profile_id","store_name","dt")]
    }
    if(notimelimit&!nothreshold){
      smart_mall_track_data_oneday_atime = smart_mall_track_data[time_duration>=threshold,.(freq = .N),by = c("profile_id","store_name","dt")]
    }
    if(!notimelimit&nothreshold){
      smart_mall_track_data_oneday_atime = smart_mall_track_data[dt>=startdate&dt<=enddate,.(freq = .N),by = c("profile_id","store_name","dt")]
    }
    if(!notimelimit&!nothreshold){
      smart_mall_track_data_oneday_atime = smart_mall_track_data[time_duration>=threshold & dt>=startdate&dt<=enddate,.(freq = .N),by = c("profile_id","store_name","dt")]
    }
    #you can combine profile_id and dt together make a new unique user id
    jinqiao_shop_track_correlation = cal_correlation_edited(smart_mall_track_data_oneday_atime,"profile_id","store_name")
    jinqiao_shop_track_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_shop_track_correlation,21)
    if(notimelimit&nothreshold){
      write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_track_correlation_melt.csv")
    }
    if(notimelimit&!nothreshold){
      write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,paste0("~/data/jinqiao_shop_track_correlation_melt_",threshold,"min.csv"))
    }
    if(!notimelimit&nothreshold){
      write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_track_correlation_melt_day.csv")
    }
    if(!notimelimit&!nothreshold){
      write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,paste0("~/data/jinqiao_shop_track_correlation_melt_day_",threshold,"min.csv"))
    }
    return(jinqiao_shop_track_correlation_melt_ordered_trunk)
  }
  if(type == "brand"){
    smart_mall_track_data = smart_mall_track_data[event_type == 0,]
    smart_mall_track_data$store_id = as.character(smart_mall_track_data$store_id)
    smart_mall_track_data_with_brand = merge(smart_mall_track_data,store_brand_dict,by.x = "store_id",by.y = "shop_id",all.x = TRUE)
    if(notimelimit&nothreshold)
      smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[,.(freq = .N),by = c("profile_id","cont_main_brand_name","dt")]
    if(notimelimit&!nothreshold)
      smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[time_duration>threshold,.(freq = .N),by = c("profile_id","cont_main_brand_name","dt")]
    if(!notimelimit&nothreshold)
      smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[dt>=startdate&dt<=enddate,.(freq = .N),by = c("profile_id","cont_main_brand_name","dt")]
    if(!notimelimit&!nothreshold)
      smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[time_duration>threshold&dt>=startdate&dt<=enddate,.(freq = .N),by = c("profile_id","cont_main_brand_name","dt")]
    jinqiao_brand_track_correlation = cal_correlation_edited(smart_mall_track_data_with_brand_oneday_atime,"profile_id","cont_main_brand_name")
    jinqiao_brand_track_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_brand_track_correlation,21)
    if(notimelimit&nothreshold){
      write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_track_correlation_melt.csv")
    }
    if(notimelimit&!nothreshold){
      write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,paste0("~/data/jinqiao_brand_track_correlation_melt_",threshold,"min.csv"))
    }
    if(!notimelimit&nothreshold){
      write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_track_correlation_melt_day.csv")
    }
    if(!notimelimit&!nothreshold){
      write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,paste0("~/data/jinqiao_brand_track_correlation_melt_day_",threshold,"min.csv"))
    }
    return(jinqiao_brand_track_correlation_melt_ordered_trunk)
  }
}

get_store_brand_dict = function(){
  store_brand_dict = jinqiao_sale_data[shop_id!="",c("shop_id","shop_name","contract_code","cont_main_brand_name")]
  # store_brand_dict = store_brand_dict_raw[!duplicated(store_brand_dict[,c("shop_id","cont_main_brand_name")]),]
  store_brand_dict = store_brand_dict[!duplicated(store_brand_dict$shop_id),]
  return(store_brand_dict)
}


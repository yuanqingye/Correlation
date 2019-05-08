source("~/Rfile/R_hive.R") ##!!need to include that file
source('~/Rfile/R_impala.R',encoding = 'UTF-8')

# 根据订单来找关联度
jinqiao_sale_data_sql = "select ordr_date,prod_name,cont_main_brand_name,mall_name,shop_id,shop_name,contract_code,booth_id,booth_desc,partner_name,open_id,cust_id,act_amt,cust_name,mobile from dl.fct_ordr where act_amt > 0 and ordr_status not in ('1','7','19','Z','X') and mall_name = '上海金桥商场'"
jinqiao_sale_data = read_data_hive_general(jinqiao_sale_data_sql)
# data = jinqiao_sale_data

jinqiao_brand_correlation = cal_correlation_edited(jinqiao_sale_data,"mobile","cont_main_brand_name")
jinqiao_shop_correlation = cal_correlation_edited(jinqiao_sale_data,"mobile","shop_name")

jinqiao_brand_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_brand_correlation)
jinqiao_shop_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_shop_correlation)
write.csv(jinqiao_brand_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_correlation_melt.csv")
write.csv(jinqiao_shop_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_correlation_melt.csv")


# jinqiao_trace_data_sql =  ""
# jinqiao_trace_data = read_data_impala_general(jinqiao_trace_data_sql)

load("~/R_Projects/sale_model_standard/smart_mall_track_data_list_second.RData")
load("~/R_Projects/sale_model_standard/smart_mall_list.RData", verbose = TRUE)
smart_mall_track_data_jan = rbindlist(smart_mall_track_data_list)
smart_mall_track_data_feb = rbindlist(smart_mall_list)
smart_mall_track_data_apr = rbindlist(smart_mall_list)
smart_mall_track_data = rbind(smart_mall_track_data_jan,smart_mall_track_data_feb)
rm(smart_mall_track_data_jan,smart_mall_track_data_feb)

smart_mall_track_data = smart_mall_track_data[event_type == 0,]
# smart_mall_track_data$time_interval = difftime(smart_mall_track_data$exit_time,smart_mall_track_data$enter_time)
smart_mall_track_data$time_duration = difftime(smart_mall_track_data$exit_time,smart_mall_track_data$enter_time,units = "mins")
smart_mall_track_data_oneday_atime = smart_mall_track_data[time_duration>2,.(freq = .N),by = c("rsprofileid","store_name","dt")]
smart_mall_track_data_oneday_atime = smart_mall_track_data[,.(freq = .N),by = c("rsprofileid","store_name","dt")]
smart_mall_track_data_oneday_atime = smart_mall_track_data[time_duration>2&dt>="20190101"&dt<="20190115",.(freq = .N),by = c("rsprofileid","store_name","dt")]

source('~/R_Projects/sale_model_standard/Rfile/function.R', encoding = 'UTF-8')
contract = get_contract_data("上海金桥商场")
contract = contract[,c("CONTRACT_CODE","BRAND_NAME")]
store_brand_dict_raw = jinqiao_sale_data[shop_id!="",c("shop_id","shop_name","contract_code","cont_main_brand_name")]
# store_brand_dict = store_brand_dict_raw[!duplicated(store_brand_dict[,c("shop_id","cont_main_brand_name")]),]
store_brand_dict = store_brand_dict[!duplicated(store_brand_dict$shop_id),]
smart_mall_track_data$store_id = as.character(smart_mall_track_data$store_id)
smart_mall_track_data_with_brand = merge(smart_mall_track_data,store_brand_dict,by.x = "store_id",by.y = "shop_id",all.x = TRUE)
smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[time_duration>2,.(freq = .N),by = c("rsprofileid","cont_main_brand_name","dt")]
smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[,.(freq = .N),by = c("rsprofileid","cont_main_brand_name","dt")]
smart_mall_track_data_with_brand_oneday_atime = smart_mall_track_data_with_brand[time_duration>2&dt>="20190101"&dt<="20190115",.(freq = .N),by = c("rsprofileid","cont_main_brand_name","dt")]

jinqiao_shop_track_correlation = cal_correlation_edited(smart_mall_track_data_oneday_atime,"rsprofileid","store_name")
jinqiao_shop_track_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_shop_track_correlation,21)
jinqiao_brand_track_correlation = cal_correlation_edited(smart_mall_track_data_with_brand_oneday_atime,"rsprofileid","cont_main_brand_name")
jinqiao_brand_track_correlation_melt_ordered_trunk = get_ordered_correlation_table(jinqiao_brand_track_correlation,21)

write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_track_correlation_melt.csv")
write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_track_correlation_melt.csv")
write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_track_correlation_melt_2min.csv")
write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_track_correlation_melt_2min.csv")
write.csv(jinqiao_shop_track_correlation_melt_ordered_trunk,"~/data/jinqiao_shop_track_correlation_melt_15day_2min.csv")
write.csv(jinqiao_brand_track_correlation_melt_ordered_trunk,"~/data/jinqiao_brand_track_correlation_melt_15day_2min.csv")

ptm = proc.time()
temp1 = strptime(smart_mall_track_data$exit_time,"%Y-%m-%d %H:%M:%S")
intv = proc.time() - ptm

library(lubridate)
ptm = proc.time()
temp1 = ymd_hms(smart_mall_track_data$exit_time)
temp0 = ymd_hms(smart_mall_track_data$enter_time)
intv = proc.time() - ptm
temp = difftime(temp1,temp0,units = "min")
temp = temp1 - temp0

starttime = ymd_hms(paste(dat[,1], dat[,2]))
endtime = ymd_hms(paste(dat[,3], dat[,4]))
interval = difftime(endtime,starttime,units = "secs")

difftime(paste(dat[,3], dat[,4]),paste(dat[,1], dat[,2]),units = "secs")

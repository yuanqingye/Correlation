source('~/R_Projects/sale_model_standard/R_scripts_functions/supportingfunctions.R', encoding = 'UTF-8')
source("~/Rfile/R_hive.R") ##!!need to include that file
source('~/Rfile/R_impala.R',encoding = 'UTF-8')

#get correlation from order data
jinqiao_sale_data = get_order_data()
jinqiao_shop_correlation_melt_ordered_trunk = get_mall_correlation_result_using_order("shop")
jinqiao_brand_correlation_melt_ordered_trunk = get_mall_correlation_result_using_order("brand")

for(dt in as.character(20190315:20190331))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_wisdowmall_store_track_dt where dt = '",dt,"'")
  smart_mall_track_data_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_track_data_list,file = "./smart_mall_track_data_march.RData")
  jgc()
}

file.create("./smart_mall_apr_list.RData")
for(dt in as.character(ymd(20190401:20190430)))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_db_aimalldigital_store_traffic_dt where enter_time like '",dt,"%'")
  smart_mall_apr_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_apr_list,file = "./smart_mall_apr_list.RData")
  jgc()
}

mon = "apr"
smart_mall_track_data = get_tracking_data(mon)
#this need latest jinqiao_sale_data
store_brand_dict = get_store_brand_dict()
library(lubridate)
temp1 = ymd_hms(smart_mall_track_data$exit_time)
temp0 = ymd_hms(smart_mall_track_data$enter_time)
smart_mall_track_data$time_duration = difftime(temp1,temp0,units = "min")
rm(temp1,temp0)
smart_mall_track_data$dt = str_sub(smart_mall_track_data$enter_time,1,10)

jinqiao_shop_track_correlation_melt_ordered_trunk = get_mall_correlation_result_using_track(smart_mall_track_data,"shop",TRUE,TRUE,threshold=0)
jinqiao_shop_track_correlation_melt_ordered_trunk0 = get_mall_correlation_result_using_track(smart_mall_track_data,"shop",FALSE,TRUE,threshold=0,startdate = "2019-04-15",enddate = "2019-04-30")
jinqiao_shop_track_correlation_melt_ordered_trunk1 = get_mall_correlation_result_using_track(smart_mall_track_data,"shop",FALSE,FALSE,threshold=1,startdate = "2019-04-15",enddate = "2019-04-30")
jinqiao_shop_track_correlation_melt_ordered_trunk2 = get_mall_correlation_result_using_track(smart_mall_track_data,"shop",FALSE,FALSE,threshold=2,startdate = "2019-04-15",enddate = "2019-04-30")

jinqiao_brand_track_correlation_melt_ordered_trunk = get_mall_correlation_result_using_track(smart_mall_track_data,"brand",TRUE,TRUE,threshold=0)
jinqiao_brand_track_correlation_melt_ordered_trunk0 = get_mall_correlation_result_using_track(smart_mall_track_data,"brand",FALSE,TRUE,threshold=0,startdate = "2019-04-15",enddate = "2019-04-30")
jinqiao_brand_track_correlation_melt_ordered_trunk1 = get_mall_correlation_result_using_track(smart_mall_track_data,"brand",FALSE,FALSE,threshold=1,startdate = "2019-04-15",enddate = "2019-04-30")
jinqiao_brand_track_correlation_melt_ordered_trunk2 = get_mall_correlation_result_using_track(smart_mall_track_data,"brand",FALSE,FALSE,threshold=2,startdate = "2019-04-15",enddate = "2019-04-30")

#get rid of big object
rm(smart_mall_track_data,smart_mall_track_data_list,smart_mall_track_data_with_brand)
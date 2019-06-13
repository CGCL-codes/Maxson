package org.apache.spark.examples.schema

/**
  * @author zyp
  */
class Allcase {

}


case class dws_rsm_dm_credit_huabei_mysql2odps_dd(user_id:String,
                                                  phone:String,
                                                  gmt_create:String,
                                                  gmt_modified:String,
                                                  topten_person:String,
                                                  start_nickname:String,
                                                  end_nickname:String,
                                                  input:String,
                                                  result:String)

case class ods_pcic_dmc_model_data(tnt_inst_id:String,
                                   id:String,
                                   gmt_create:String,
                                   gmt_creator:String,
                                   gmt_modified:String,
                                   gmt_modifier:String,
                                   category:String,
                                   entity_type:String,
                                   entity_name:String,
                                   entity_code:String,
                                   data_provider:String,
                                   data_time:String,
                                   object_content:String,
                                   metadata:String,
                                   data_org_id:String,
                                   collect_execution_id:String) //1

case class ods_pdm_order_operate(operate_no:String,
                                 data_col:String)  //2.0
case class ods_lnia_org_info(inst_code:String,
                             ip_id:String,
                             ip_role_id:String,
                             in_acct_no:String,
                             in_acct_tp:String,
                             out_acct_no:String,
                             out_acct_tp:String)  //2.1
case class ods_parm_d(
                       parm_t_code:String,
                       crtor:String,
                       last_moder:String,
                       gmt_create:String,
                       gmt_modified:String,
                       json_data:String)   //3

case class ods_parm_d2(
                        parm_t_code:String,
                        crtor:String,
                        last_moder:String,
                        gmt_create:String,
                        gmt_modified:String,
                        json_data:String)   //4

case class s_gd_poi_base(poiid:String,
                         json_str:String) //5
case class cms_ces_generic_review_df(id:BigInt,
                                     resource_id:String,
                                     task_type:String
                                    ) //6.0
case class s_generic_task_edit_result_json(data:String) //6.1

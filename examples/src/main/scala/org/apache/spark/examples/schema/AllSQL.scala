package org.apache.spark.examples.schema

/**
  * @author zyp
  */
object AllSQL {

  val sql1 = """SELECT  id
               |        ,gmt_create
               |        ,gmt_modified
               |        ,entity_type
               |        ,entity_name
               |        ,entity_code
               |        ,data_org_id
               |        ,GET_JSON_OBJECT(object_content, '$.dataTime') AS dataTime
               |        ,GET_JSON_OBJECT(object_content, '$.seqId') AS seqId
               |        ,GET_JSON_OBJECT(object_content, '$.finalDecision') AS finalDecision
               |        ,GET_JSON_OBJECT(object_content, '$.finalScore') AS finalScore
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdZhldspt') AS extIdZhldspt
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileTgpt') AS extMobileTgpt
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdYhxfjrgs') AS extIdYhxfjrgs
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileDxxfjrgs'
               |        ) AS extMobileDxxfjrgs
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdRsbx') AS extIdRsbx
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileJtgj') AS extMobileJtgj
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileHkgs') AS extMobileHkgs
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdYhgryw') AS extIdYhgryw
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdYhxwdk') AS extIdYhxwdk
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileCcbx') AS extMobileCcbx
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileGg') AS extMobileGg
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileYhgryw'
               |        ) AS extMobileYhgryw
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileZxyh') AS extMobileZxyh
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileCsqcjr'
               |        ) AS extMobileCsqcjr
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdZxyh') AS extIdZxyh
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileFdcjr'
               |        ) AS extMobileFdcjr
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdO2o') AS extIdO2o
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileXekdgs'
               |        ) AS extMobileXekdgs
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdQczl') AS extIdQczl
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdDb') AS extIdDb
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdHljy') AS extIdHljy
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileXnhbpt'
               |        ) AS extMobileXnhbpt
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileDsffws'
               |        ) AS extMobileDsffws
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdKjdspt') AS extIdKjdspt
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdLcjg') AS extIdLcjg
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdCsqcjr') AS extIdCsqcjr
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileWsyh') AS extMobileWsyh
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdRzzl') AS extIdRzzl
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdSjwz') AS extIdSjwz
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileO2o') AS extMobileO2o
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdXnhb') AS extIdXnhb
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdWl') AS extIdWl
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileZcpt') AS extMobileZcpt
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdDsfzf') AS extIdDsfzf
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileHlwjrmh'
               |        ) AS extMobileHlwjrmh
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdZcpt') AS extIdZcpt
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileWl') AS extMobileWl
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileKjdspt'
               |        ) AS extMobileKjdsp
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileYhdgyw'
               |        ) AS extMobileYhdgyw
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdYhdgyw') AS extIdYhdgyw
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileYbxffq'
               |        ) AS extMobileYbxffq
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdDsffws') AS extIdDsffws
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdGg') AS extIdGg
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdWsyh') AS extIdWsyh0
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileXykzx'
               |        ) AS extMobileXykzx
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdYbxffqpt') AS extIdYbxffqp0
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileBjldspt'
               |        ) AS extMobileBjldsp
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdZczr') AS extIdZczr0
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileSjwz') AS extMobileSjwz
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileRzzl') AS extMobileRzzl
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileXdyq') AS extMobileXdyq
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileDsfzf'
               |        ) AS extMobileDsfzf
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileLcjg') AS extMobileLcjg
               |        ,GET_JSON_OBJECT(
               |            object_content
               |            ,'$.outputFields.extMobileCzldspt'
               |        ) AS extMobileCzldsp
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extMobileYx') AS extMobileYx
               |        ,GET_JSON_OBJECT(object_content, '$.outputFields.extIdHlwjrmh') AS extIdHlwjrmh
               |        ,GET_JSON_OBJECT(object_content, '$.accountName') AS accountName
               |        ,GET_JSON_OBJECT(object_content, '$.idNumber') AS idNumber
               |        ,GET_JSON_OBJECT(object_content, '$.accountMobile') AS accountMobile
               |	FROM    ods_pcic_dmc_model_data
               |	WHERE     category != 'TONGDUN_INFO'""".stripMargin


/**sql2 有数组形式**/
  val sql2 = """select * from
               |	(
               |        SELECT
               |                operate_no,
               |                get_json_object(data_col,'$.instData.instParties\[0].type') as orgType,
               |                get_json_object(data_col,'$.instData.instParties\[0].code') as inst_code,
               |                get_json_object(data_col,'$.instData.instParties\[0].ipId') as ip_id,
               |                get_json_object(data_col,'$.instData.instParties\[0].ipRoleId') as ip_role_id,
               |                get_json_object(data_col,'$.instData.instParties\[0].accountType') as out_acct_type,
               |                get_json_object(data_col,'$.instData.instParties\[0].accountNo') as out_acct_no,
               |                get_json_object(data_col,'$.instData.instParties\[0].accountType') as in_acct_type,
               |                get_json_object(data_col,'$.instData.instParties\[0].accountNo') as in_acct_no
               |        FROM ods_pdm_order_operate
               |        WHERE
               |
               |                 get_json_object(data_col,'$.instData.instParties\[0].code') != '890001'
               |                and get_json_object(data_col,'$.instData.instParties\[0].type') != 'CUSTORG'
               |	) t1
               |	LEFT OUTER JOIN (
               |        SELECT
               |                inst_code, ip_id, ip_role_id, in_acct_no, in_acct_tp, out_acct_no, out_acct_tp
               |        FROM ods_lnia_org_info
               |
               |	) t2
               |	ON t1.ip_id = t2.ip_id
               |	WHERE t1.ip_id is null
               |                or t2.ip_id is null
               |                or t1.ip_role_id != t2.ip_role_id
               |                or t1.inst_code != t2.inst_code
               |                or t1.in_acct_no != t2.in_acct_no
               |                or t1.in_acct_type != t2.in_acct_tp
               |                or t1.out_acct_no != t2.out_acct_no
               |                or t1.out_acct_type != t2.out_acct_tp""".stripMargin


  val sql3 = """select
               |    parm_t_code,
               |    GET_JSON_OBJECT(json_data,'$.dataContentQualifierDesc') AS dataContentQualifierDesc,
               |    GET_JSON_OBJECT(json_data,'$.dataContentDepartmentId') AS  dataContentDepartmentId,
               |    GET_JSON_OBJECT(json_data,'$.dataContentDomainId') AS  dataContentDomainId,
               |    GET_JSON_OBJECT(json_data,'$.gmtModify') AS  gmtModify,
               |    GET_JSON_OBJECT(json_data,'$.ownerName') AS  ownerName,
               |    GET_JSON_OBJECT(json_data,'$.ModifierName') AS  ModifierName,
               |    GET_JSON_OBJECT(json_data,'$.dataContentQualifierEnName') AS  dataContentQualifierEnName,
               |    GET_JSON_OBJECT(json_data,'$.dataContentQualifierId') AS  dataContentQualifierId,
               |    GET_JSON_OBJECT(json_data,'$.gmtCreate') AS  gmtCreate,
               |    GET_JSON_OBJECT(json_data,'$.dataContentCompositeQualifierIds') AS  dataContentCompositeQualifierIds,
               |    GET_JSON_OBJECT(json_data,'$.dataContentQualifierCnName') AS  dataContentQualifierCnName,
               |     crtor,
               |     last_moder,
               |     gmt_create,
               |     gmt_modified,
               |     json_data
               |     from ods_parm_d
               |	where parm_t_code != '120001235'""".stripMargin

  val sql4 = """select
               |	'20190104' as thedate
               |	,parm_t_code
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimAttributeEnName') as dataContentDimAttributeEnName
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDepartmentId') as dataContentDepartmentId
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimAttributeCnName') as dataContentDimAttributeCnName
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimId') as dataContentDimId
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimAttributeId') as dataContentDimAttributeId
               |	,GET_JSON_OBJECT(json_data,'$.dataClassification') as dataClassification
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimAttributeHierachyId') as dataContentDimAttributeHierachyId
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDimAttributeDesc') as dataContentDimAttributeDesc
               |	,GET_JSON_OBJECT(json_data,'$.dataContentParentDimAttributeId') as dataContentParentDimAttributeId
               |	,GET_JSON_OBJECT(json_data,'$.dataContentDataType') as dataContentDataType
               |	,crtor
               |	,last_moder
               |	,gmt_create
               |	,gmt_modified
               |	,json_data
               |	  from ods_parm_d2
               |	where parm_t_code != '120001242'""".stripMargin

  val sql5 = """select aa.poiid,
               |	                aa.name,
               |	                aa.update_flag as newupdate_flag,
               |	                bb.update_flag as oldupdate_flag,
               |	                aa.update_flag_source as newupdate_flagsource,
               |	                bb.update_flag_source as oldupdate_flagsource,
               |	                case
               |	                        when aa.update_flag ='d' and (bb.update_flag ='a' or bb.update_flag ='u') then "删除"
               |	                        when bb.update_flag  is null and aa.update_flag is not null then "新增"
               |	                        when (aa.update_flag = 'u' or aa.update_flag ='a') and (aa.update_flag = 'u' or aa.update_flag ='a') then "保持有"
               |	                        when aa.update_flag = 'd' and bb.update_flag = 'd' then "保持删除"
               |	                        when aa.update_flag = 'u' and bb.update_flag = 'd' then "二次上线"
               |	                else "错误"
               |	                end description,
               |	                case
               |	                        when aa.update_flag ='d' and (bb.update_flag ='a' or bb.update_flag ='u') then 1
               |	                        when bb.update_flag  is null and aa.update_flag is not null then 2
               |	                        when (aa.update_flag = 'u' or aa.update_flag ='a') and bb.update_flag  = 'u' then 3
               |	                        when aa.update_flag = 'd' and bb.update_flag = 'd' then 4
               |	                        when aa.update_flag = 'u' and bb.update_flag = 'd' then 5
               |	                else 6
               |	                end description_code
               |	from
               |	(
               |	        SELECT poiid
               |	                , get_json_object(json_str,'$.update_flag') as update_flag
               |	                , get_json_object(json_str,'$.merged_status') as merged_status
               |	                , get_json_object(json_str,'$.baseinfo.name') as name
               |	                , get_json_object(json_str,'$.baseinfo.address') as address
               |	                , get_json_object(json_str,'$.baseinfo.x') as x
               |	                , get_json_object(json_str,'$.baseinfo.y') as y
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.name')  as name_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.address') as address_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.navi') as navi_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.x') as xy_source
               |	                ,case
               |	                        when get_json_object(json_str,'$.update_flag') = 'd' or get_json_object(json_str,'$.merged_status') = '1' then get_json_object(json_str, '$.baseinfo.from.src_type')
               |	                        else get_json_object(json_str, '$.baseinfo.from_field.opt_type')
               |	                 end as update_flag_source
               |	        FROM s_gd_poi_base
               |
               |	) aa
               |	full outer join
               |	(
               |	        SELECT poiid
               |	                , get_json_object(json_str,'$.update_flag') as update_flag
               |	                , get_json_object(json_str,'$.merged_status') as merged_status
               |	                , get_json_object(json_str,'$.baseinfo.name') as name
               |	                , get_json_object(json_str,'$.baseinfo.address') as address
               |	                , get_json_object(json_str,'$.baseinfo.x') as x
               |	                , get_json_object(json_str,'$.baseinfo.y') as y
               |
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.name')  as name_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.address') as address_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.navi') as navi_source
               |	                ,get_json_object(json_str, '$.baseinfo.from_field.x') as xy_source
               |	                ,case
               |	                        when get_json_object(json_str,'$.update_flag') != 'd' or get_json_object(json_str,'$.merged_status') = '1' then get_json_object(json_str, '$.baseinfo.from.src_type')
               |	                        else get_json_object(json_str, '$.baseinfo.from_field.opt_type')
               |	                 end as update_flag_source
               |	        FROM s_gd_poi_base
               |
               |	) bb on aa.poiid =bb.poiid""".stripMargin


  val sql6 = """select review_type,
               |	    resource_id,
               |	    task_type,
               |	    result_tag,
               |	    RANK() OVER(PARTITION BY review_type, resource_id, task_type ORDER BY workflow_id asc) AS result_order
               |	    from (
               |	        select
               |	        'edit' as review_type,
               |	        a.resource_id,
               |	        a.task_type,
               |	        case when a.task_type != 'expire_tel_around' then GET_JSON_OBJECT(b.data, '$.result.poi_status.verify')
               |	        else '' end as result_tag,
               |	        a.id as workflow_id
               |	        from cms_ces_generic_review_df a
               |	        left outer join s_generic_task_edit_result_json b
               |	        on a.id = GET_JSON_OBJECT(b.data, '$.id')
               |	        where task_type not in ('expire_pic_verify','expire_tel_self','expire_tel_around','expire_web')
               |
               |	        union all
               |
               |	         select
               |	        'inreview_edit' as review_type,
               |	        a.resource_id,
               |	        a.task_type,
               |	        case when a.task_type != 'expire_tel_around' then GET_JSON_OBJECT(b.data, '$.result.poi_status.verify')
               |	        else '' end as result_tag,
               |	        a.id as workflow_id
               |	        from cms_ces_generic_review_df a
               |	        left outer join s_generic_task_edit_result_json b
               |	        on a.id = GET_JSON_OBJECT(b.data, '$.id')
               |	        where task_type not in ('expire_pic_verify','expire_tel_self','expire_tel_around','expire_web')
               |	    ) a""".stripMargin
}

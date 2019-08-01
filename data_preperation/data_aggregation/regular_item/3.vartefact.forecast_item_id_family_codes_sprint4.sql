/*
Description: Create table "{database}.forecast_item_id_family_codes_sprint4",
             create a table where with item_id, we can get group_family_code, family_code, sub_family_code information
             and item description

Input:  {database}.forecast_itemid_list_threebrands_sprint4
        ods.p4md_mstms
        ods.p4md_itmgld

Output: {database}.forecast_item_id_family_codes_sprint4

Created: 2019-07-01
*/
-- drop table if exists {database}.forecast_item_id_family_codes_sprint4;

create table {database}.forecast_item_id_family_codes_sprint4 as
with item_id_list as (
    select
        a.*,
        b.itmitmid,
        b.itmedesc,
        b.itmldesc,
        b.itmstkun,
        b.itmpack,
        b.itmcapa,
        b.itmcaput,
        concat(b.itmdpcd, substr(itmitmcd,1,1)) AS group_family_code,
        concat(b.itmdpcd, substr(itmitmcd,1,2)) AS family_code,
        concat(b.itmdpcd, substr(itmitmcd,1,3)) AS sub_family_code

    from {database}.forecast_itemid_list_threebrands_sprint4 a
    left join (select * from ods.p4md_itmgld) b
    on b.itmitmid = a.item_id
),

add_group_family_desc as (
    select
        a.*,
        group_family_info.group_family_edesc,
        group_family_info.group_family_ldesc

    from item_id_list a

    LEFT JOIN (
                SELECT
                    concat(mstdpcd, mstmscd) AS group_family_code,
                    mstedesc AS group_family_edesc,
                    mstldesc AS group_family_ldesc

                FROM ods.p4md_mstms
                WHERE mstlevel = 1
                )  group_family_info
    ON a.group_family_code = group_family_info.group_family_code
),

add_family_desc as (
    select
        a.*,
        family_info.family_edesc,
        family_info.family_ldesc

    from add_group_family_desc a
    LEFT JOIN (
                    SELECT
                        concat(mstdpcd, mstmscd) AS family_code,
                        mstedesc AS family_edesc,
                        mstldesc AS family_ldesc

                    FROM ods.p4md_mstms
                    WHERE mstlevel = 2
                )  family_info
        ON a.family_code = family_info.family_code
),

add_sub_family_desc as (
    select
        a.*,
        sub_family_info.sub_family_edesc,
        sub_family_info.sub_family_ldesc

    from add_family_desc a
    LEFT JOIN (
                    SELECT
                        concat(mstdpcd, mstmscd) AS sub_family_code,
                        mstedesc AS sub_family_edesc,
                        mstldesc AS sub_family_ldesc

                    FROM ods.p4md_mstms
                    WHERE mstlevel = 3
                )  sub_family_info
        ON a.sub_family_code = sub_family_info.sub_family_code

)
select *
from add_sub_family_desc;

-- invalidate metadata {database}.forecast_item_id_family_codes_sprint4;

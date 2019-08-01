/* This table is created because the fds.p4cm_item_map is not completed. 
-- Will be replaced by fds.p4cm_item_map once it is completed.
-- Input:   ods.dim_calendar
            {database}.forecast_trxn_v7_full_item_id_sprint4
            fds.p4cm_item_map
    Output: {database}.p4cm_item_map_complete 
*/
create table {database}.p4cm_item_map_complete as 
with
    complete_date as (
        select
            date_key
        from ods.dim_calendar
        where date_key >= cast({starting_date} as string)
        and date_key < cast({ending_date} as string) 
    ),
    item_list as (
        select
            item_id,
            sub_id
            -- max(item_code) as item_code,
            -- max(sub_code) as sub_code
        from {database}.forecast_trxn_v7_full_item_id_sprint4
        group by item_id, sub_id
    ),
    item_day_structure as (
        select
            date_key,
            item_id,
            sub_id
        from complete_date
        left join item_list
            on 1=1
    ),
    unique_mapping as (
      select
        date_key,
        item_id,
        sub_id,
        max(item_code) as item_code,
        max(sub_code) as sub_code,
        max(dept_code) as dept_code
      from fds.p4cm_item_map
      where item_id in (select item_id from item_list)
      and sub_id in (select sub_id from item_list)
      group by date_key,item_id,sub_id
    ),
    item_day as (
        select
            empty_.date_key,
            empty_.item_id,
            empty_.sub_id,
            map_.item_code,
            map_.sub_code,
            map_.dept_code,
            (case
                when map_.item_code is null then 0
                else 1
            end) as if_value_flag
        from item_day_structure empty_
        left join unique_mapping map_
            on empty_.date_key = map_.date_key
            and empty_.item_id = map_.item_id
            and empty_.sub_id = map_.sub_id
    ),
    item_day_accumulation as (
        select
            sum(if_value_flag) over (partition by item_id,sub_id order by date_key asc) as value_id_raw,
            *
        from item_day
    ),
    item_day_accumulation_modify as (
        select
            (case
                when value_id_raw = 0 then 1
                else value_id_raw
            end) as value_id,
            *
        from item_day_accumulation
    ),
    mapping_value as (
        select
            distinct
            value_id,
            item_id,
            sub_id,
            item_code,
            sub_code,
            dept_code,
            date_key
        from item_day_accumulation_modify
        where if_value_flag = 1
    ),
    item_day_final as (
        select
            item_day.date_key,
            item_day.item_id,
            item_day.sub_id,
            mapping.item_code,
            mapping.sub_code,
            mapping.dept_code
        from item_day_accumulation_modify item_day
        left join mapping_value mapping
            on item_day.item_id = mapping.item_id
            and item_day.sub_id = mapping.sub_id
            and item_day.value_id = mapping.value_id
    )
select * from item_day_final;

with deleted as (
    select 
        id,
        partitionChangeDate,
        changeDate,
        globalObjectKey,
        operation
    from red_red_filtered
    where 
        operation = 'Delete'
        and classified_metaData_classifiedId IS NULL
        and partitionchangedate>=to_date('{first_day_past}') 
),

non_deleted as (
    select
        id, partitionChangeDate, changeDate, globalObjectKey, operation, {extra_columns_wo_prefix}
    from red_red_filtered
    where 
        operation != 'Delete'
        and classified_metaData_classifiedId IS NOT NULL
),

joined as (
    select
        a.id, a.partitionChangeDate, a.changeDate, a.globalObjectKey, a.operation, b.changeDate as b_changeDate, {extra_columns_with_prefix},
        row_number() OVER (PARTITION BY a.globalObjectKey, a.changeDate ORDER BY b.changeDate DESC) as rank
    from deleted a
    inner join non_deleted b
        on a.globalObjectKey = b.globalObjectKey
        and a.changeDate >= b.changeDate
),

final_deleted as (
    select 
        id, partitionChangeDate, changeDate, globalObjectKey, operation, {extra_columns_wo_prefix}
    from joined
    where
        rank = 1
),

final_non_deleted as (
    select 
        id, partitionChangeDate, changeDate, globalObjectKey, operation, {extra_columns_wo_prefix}
    from non_deleted
    where 
        partitionchangedate>=to_date('{first_day_past}') 
),

final_union as (
    select
        id, partitionChangeDate, changeDate, globalObjectKey, operation, {extra_columns_wo_prefix}
    from final_deleted
    union all
    select
        id, partitionChangeDate, changeDate, globalObjectKey, operation, {extra_columns_wo_prefix}
    from final_non_deleted
)

select *
from final_union
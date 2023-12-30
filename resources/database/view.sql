create or replace view bt_accuracy_summary_rnk
as
select
    bt.*,
    CASE
        WHEN RANK() OVER (PARTITION BY bt.scrip, bt.strategy ORDER BY bt.trade_date DESC) = 1
        THEN 'Y' ELSE 'N'
    END AS latest
from bt_accuracy_summary bt;

create or replace view base_bt_accuracy_summary
as
select rnk.*
from bt_accuracy_summary_rnk rnk
where rnk.run_type='BASE';

create or replace view rf_bt_accuracy_summary
as
select rnk.*
from bt_accuracy_summary_rnk rnk
where rnk.run_type='REWARD_FACTOR';

create or replace view base_bt_accuracy_trades
as
select *
from bt_accuracy_trades
where run_type='BASE';


create or replace view rf_bt_accuracy_trades
as
select *
from bt_accuracy_trades
where run_type='REWARD_FACTOR';
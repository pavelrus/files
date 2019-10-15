drop table if exists #biometrics; -- ���������
create table #biometrics
(
  id_trade_point int not null, -- Id �������� �����
  id_employee int not null, -- Id ����������
  ts datetime not null, -- ���� � ����� �������
  [state] varchar(3) not null -- ��� ����������� ����������
);

set dateformat dmy;

insert #biometrics
values
  /*(1,	123,	'19.06.2019 10:00',	'ON'),
  (1,	123,	'19.06.2019 12:00',	'ON'),
  (1,	123,	'19.06.2019 14:00',	'OFF'),
  (1,	123,	'19.06.2019 15:00',	'ON'),
  (1,	123,	'19.06.2019 17:55',	'OFF'),
  (1,	123,	'19.06.2019 18:59',	'OFF'),
  (1,	123,	'19.06.2019 19:00',	'ON')*/
  (2,	124,	'19.06.2019 08:00',	'ON'),
  (2,	124,	'19.06.2019 10:14',	'ON'),
  (2,	124,	'19.06.2019 10:16',	'OFF'),
  (2,	124,	'19.06.2019 10:17',	'ON'),
  (2,	124,	'19.06.2019 11:17',	'OFF'),
  (2,	124,	'19.06.2019 11:17',	'OFF')
  /*(3,	125,	'19.06.2019 18:17',	'ON'),
  (3,	125,	'19.06.2019 19:17',	'OFF'),
  (3,	125,	'20.06.2019 09:20',	'ON'),
  (3,	125,	'20.06.2019 09:45',	'OFF'),
  (3,	125,	'20.06.2019 10:00',	'ON'),
  (3,	125,	'20.06.2019 10:25',	'OFF');*/
  /*(3,	125,	'19.06.2019 09:00',	'ON'),
  (3,	125,	'19.06.2019 10:00',	'ON'),
  (3,	125,	'19.06.2019 10:10',	'OFF')*/;

-- 
with cte as
  (
    select
      row_number() over (partition by id_trade_point, id_employee, ts order by [state] desc) rn
    from #biometrics
  )
delete
from cte
where rn > 1;

-- �������� �����
drop table if exists #trade_point;
create table #trade_point
(
  id_trade_point int primary key,
  is_night bit not null
);

insert #trade_point
values
  (1, 0),
  (2, 0),
  (3, 1);

-- ���������� ������
drop table if exists #biometrics_refactored;
create table #biometrics_refactored
(
  id_trade_point int not null,
  id_employee int not null,
  ts datetime not null,
  [state] varchar(3) not null,
  [period] date not null,
  ts_from datetime not null,
  ts_to datetime not null,
  state_next varchar(3) not null,
  primary key (id_trade_point, id_employee, ts)
);

insert #biometrics_refactored
select
  id_trade_point,
  id_employee,
  ts,
  [state],
  ts_with_bias [period],
  dateadd(n, datepart(n, ts_with_bias), dateadd(hh, datepart(hh, ts_with_bias), '19000101')) ts_from,
  dateadd(n, datepart(n, ts_to), dateadd(hh, datepart(hh, ts_to), '19000101')) ts_to,
  state_next
from
  (
    select
      id_trade_point,
      id_employee,
      ts,
      [state],
      ts_with_bias,
      lead(dateadd(n, -1, ts_with_bias), 1, '01.01.1900 23:59') over (partition by id_trade_point, id_employee order by ts_with_bias) ts_to,
      lead([state], 1, '-') over (partition by id_trade_point, id_employee order by ts_with_bias) state_next
    from
        (
          select
            b.id_trade_point,
            b.id_employee,
            b.ts,
            b.[state],
            dateadd(hh, -10 * tp.is_night, ts) ts_with_bias
          from #biometrics b
            join #trade_point tp
              on tp.id_trade_point = b.id_trade_point
        ) t
  ) t;

-- ���������
drop table if exists #time_intervals;
create table #time_intervals
(
  ts_from datetime primary key,
  ts_to datetime not null,
  id_interval int not null,
  id_interval_with_bias int not null,
  is_night bit not null
);

;with rcte(ts) as
  (
    select convert(datetime, '01.01.1900 00:00') ts
      union all
    select dateadd(n, 15, ts) from rcte where ts < '01.01.1900 23:45'
  )
insert #time_intervals
select
  ts,
  dateadd(n, 14, ts) ts_to,
  datepart(hh, ts) * 100 + datepart(n, ts) id_interval,
  datepart(hh, dateadd(hh, 10, ts)) * 100 + datepart(n, dateadd(hh, 10, ts)) id_interval_with_bias,
  iif(datepart(hh, ts) between 9 and 20, 0, 1) is_night
from rcte;

-- ���������
select
  id_trade_point 'Id �������� �����',
  id_employee 'Id ����������',
  [date] '����',
  id_interval 'ID ���������',
  sum([minutes]) '���������� ������� �����'
  --,[period] '�������� ����'
from
  (
    select
      b.id_trade_point,
      b.id_employee,
      dateadd(day, iif(tp.is_night = 1 and i.id_interval_with_bias between 0 and 945, 1, 0), b.[period]) [date],
      b.[period],
      iif(tp.is_night = 1, i.id_interval_with_bias, i.id_interval) id_interval,
      datediff(n, iif(i.ts_from > b.ts_from, i.ts_from, b.ts_from), iif(i.ts_to > b.ts_to, b.ts_to, i.ts_to)) + 1 [minutes]
    from #biometrics_refactored b
      join #trade_point tp
        on tp.id_trade_point = b.id_trade_point
      join #time_intervals i
        on i.ts_from <= b.ts_to
          and b.ts_from <= i.ts_to
          and (i.is_night = tp.is_night or tp.is_night = 1)
    where b.[state] = 'ON'
  )t
group by
  id_trade_point,
  id_employee,
  [date],
  [period],
  id_interval
order by
  id_trade_point,
  id_employee,
  [date],
  [period],
  id_interval;

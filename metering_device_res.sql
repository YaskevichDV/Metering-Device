--
--�������� ����� ������ � ��������� � ���
--
-- Table metering_device  -- SCD2
create table metering_device(
id                      number generated always as identity primary key not null,
serial_num              varchar2(20) not null,
metering_device_type_id int not null,
ind_default             int not null,
placement_id            int not null,
flag_top                int not null,
created_dt              date default sysdate not null,
updated_dt              date
);

-- Comments
comment on table metering_device                          is '������ �����'; 

comment on column metering_device.id                      is 'ID ������� �����'; 
comment on column metering_device.serial_num              is '�������� �����'; 
comment on column metering_device.metering_device_type_id is 'ID ���� ������� ����� (spr_metering_device_type)'; 
comment on column metering_device.ind_default             is '��������� �� ��������� (�� ������ ��������� ��������)';
comment on column metering_device.placement_id            is 'ID ����� ����������'; 
comment on column metering_device.flag_top                is '������� ������������'; 
comment on column metering_device.created_dt              is '���� ������� ������������� �������'; 
comment on column metering_device.updated_dt              is '���� ��������� �������'; 
 
-- Foreign Keys
alter table metering_device
add constraint fk_m_device_type_id
  foreign key (metering_device_type_id)
    references spr_metering_device_type(id);

alter table metering_device
add constraint fk_m_device_placement_id
  foreign key (placement_id)
    references spr_metering_device_placement(id);

-- Triggers
create or replace trigger triu_metering_device 
  before insert or update 
  on metering_device
  for each row
begin
  if inserting then
    update metering_device 
       set flag_top = 0 
     where serial_num = :new.serial_num 
       and flag_top = 1;  
  end if;
  if updating then 
    :new.updated_dt := sysdate;
  end if;
end triu_metering_device;  


--
-- Table metering_device_ind
create table metering_device_ind(
id                      number generated always as identity primary key not null,
metering_device_id      number,
ind_begin               int not null,
ind_end                 int not null,
ind_date                date not null
) partition by range(ind_date)
    interval (interval '3' year)
      ( partition metering_device_ind_min values less than (to_date('01/01/2024', 'dd/mm/yyyy')) );

-- Comments
comment on table metering_device_ind                     is '��������� ������� �����'; 

comment on column metering_device_ind.id                 is 'ID ��������� ������� �����';
comment on column metering_device_ind.metering_device_id is '��������� �� ��������� (�� ������ ��������� ��������)';
comment on column metering_device_ind.ind_begin          is '��������� �� ������ ��������� �������';
comment on column metering_device_ind.ind_end            is '��������� �� ����� ��������� �������';
comment on column metering_device_ind.ind_date           is '���� ������ ���������';

-- Foreign Key
alter table metering_device_ind
add constraint fk_m_device_ind_m_d_id
  foreign key (metering_device_id)
    references metering_device(id);

-- Indices
create index ind_m_device_ind_m_d_id on metering_device_ind(metering_device_id) local; 
create index ind_m_device_ind_ind_date on metering_device_ind(to_char(ind_date, 'mmyyyy')) local; 


--
-- Table spr_metering_device_type
create table spr_metering_device_type(id number primary key not null, m_d_type_name varchar2(100));

-- Comments
comment on table spr_metering_device_type                is '��� ������� �����'; 

comment on column spr_metering_device_type.id            is 'ID ���� ������� �����'; 
comment on column spr_metering_device_type.m_d_type_name is '������������ ���� ������� �����'; 

-- Inserts
insert into spr_metering_device_type values (1, '�������� ����');
insert into spr_metering_device_type values (2, '������� ����');
insert into spr_metering_device_type values (3, '�������������');
insert into spr_metering_device_type values (4, '���');


--
-- Table spr_metering_device_placement
create table spr_metering_device_placement(id number primary key not null, m_d_placement_name varchar2(100));

-- Comments
comment on table spr_metering_device_placement                     is '����� ����������'; 

comment on column spr_metering_device_placement.id                 is 'ID ����� ����������'; 
comment on column spr_metering_device_placement.m_d_placement_name is '������������ ����� ����������'; 

-- Inserts
insert into spr_metering_device_placement values (1, '�������� ��������������� �����');
insert into spr_metering_device_placement values (2, '������� ����');
insert into spr_metering_device_placement values (3, '����������� ��������');
insert into spr_metering_device_placement values (4, '������� ����');

   
--
-- Types
create or replace type t_metering_device_type as object( 
  serial_num varchar2(20), 
  m_d_type_name varchar2(100),
  ind_begin int,
  ind_end int,
  expense int, 
  ind_default int, 
  m_d_placement_name varchar2(100)
);

create or replace type t_metering_device_table as table of t_metering_device_type;

--
-- Functions

-- Function f_get_m_device_by_placement -- PIPELINED
-- ��. ��������� :
--       p_placement_id       - ����� ���������� (ID �� �����������), 
--       p_rep_period_mon     - �������� ������ (�����), 
--       p_rep_period_year    - �������� ������ (���)
-- ���. ����� :
--       serial_num           -	�������� �����;
--       m_d_type_name        -	��� ������� �����;
--       ind_begin            -	��������� �� ������ ��������� �������;
--       ind_end              -	��������� �� ����� ��������� �������;
--       expense              -	������ (����/����/�������������) �� �������� ������;
--       ind_default          -	��������� �� ��������� (�� ������ ��������� ��������);
--       m_d_placement_name   -	����� ����������.
--
-- �����: select * from f_get_m_device_by_placement(1, 11, 2024);
--
create or replace function f_get_m_device_by_placement(p_placement int, p_rep_period_mon int, p_rep_period_year int) return t_metering_device_table pipelined
is
  begin
    for curs in (
      select serial_num, smt.m_d_type_name, min(mi.ind_begin) ind_begin, max(mi.ind_end) ind_end, max(mi.ind_end) - min(mi.ind_begin) expense, m.ind_default, smp.m_d_placement_name
        from metering_device_ind mi    
        join metering_device m 
          on m.id = mi.metering_device_id 
         and m.placement_id = p_placement
        join spr_metering_device_type smt 
          on smt.id = m.metering_device_type_id
        join spr_metering_device_placement smp 
          on smp.id = m.placement_id
       where to_char(ind_date, 'mmyyyy') = lpad(to_char(p_rep_period_mon), 2, '0')||to_char(p_rep_period_year)
       group by m.serial_num, smt.m_d_type_name, smp.m_d_placement_name, m.ind_default  
    ) 
    loop
      pipe row(t_metering_device_type(curs.serial_num, 
                                      curs.m_d_type_name, 
                                      curs.ind_begin, 
                                      curs.ind_end, 
                                      curs.expense, 
                                      curs.ind_default, 
                                      curs.m_d_placement_name));
    end loop;
end f_get_m_device_by_placement;


-- Function f_get_m_device_by_placement_c -- CURSOR
-- ��. ��������� :
--       p_placement_id       - ����� ���������� (ID �� �����������), 
--       p_rep_period_mon     - �������� ������ (�����), 
--       p_rep_period_year    - �������� ������ (���)
-- ���. ����� :
--       serial_num           -	�������� �����;
--       m_d_type_name        -	��� ������� �����;
--       ind_begin            -	��������� �� ������ ��������� �������;
--       ind_end              -	��������� �� ����� ��������� �������;
--       expense              -	������ (����/����/�������������) �� �������� ������;
--       ind_default          -	��������� �� ��������� (�� ������ ��������� ��������);
--       m_d_placement_name   -	����� ����������.
--
create or replace function f_get_m_device_by_placement_c(p_placement int, p_rep_period_mon int, p_rep_period_year int) return sys_refcursor
is
  curs sys_refcursor;
begin
  open curs for 
    select serial_num, smt.m_d_type_name, min(mi.ind_begin) ind_begin, max(mi.ind_end) ind_end, max(mi.ind_end) - min(mi.ind_begin) expense, m.ind_default, smp.m_d_placement_name
      from metering_device_ind mi    
      join metering_device m 
        on m.id = mi.metering_device_id 
       and m.placement_id = p_placement
      join spr_metering_device_type smt 
        on smt.id = m.metering_device_type_id
      join spr_metering_device_placement smp 
        on smp.id = m.placement_id
     where to_char(ind_date, 'mmyyyy') = lpad(to_char(p_rep_period_mon), 2, '0')||to_char(p_rep_period_year)
     group by m.serial_num, smt.m_d_type_name, smp.m_d_placement_name, m.ind_default;
--  
  return curs;
end f_get_m_device_by_placement_c;


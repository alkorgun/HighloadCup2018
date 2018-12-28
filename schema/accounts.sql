
create table if not exists hlcup2018.accounts (
	id UInt32,
	email String,
	fname String,
	sname String,
	interests Array(String),
	status Enum8('свободны' = 0, 'заняты' = 1, 'всё сложно' = 2),
	p_start Int32,
	p_end Int32,
	sex Enum8('f' = 0, 'm' = 1),
	phone String,
	phone_c FixedString(3),
	birth Int32,
	birth_y Int16,
	city String,
	country String,
	joined Int32,
	joined_y Int32
) engine = Memory;

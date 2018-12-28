
create table if not exists hlcup2018.likes (
	liker UInt32,
	likee UInt32,
	ts Int32
) engine = Memory;

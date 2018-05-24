select display_url, count(*) as cnt from tweet
group by 1
order by 2 desc
;
select max(tweet_sentiment) as tweet_sentiment from tweet
order by tweet_sentiment desc
;


select t1.name, 
       count_location, 
       t1.location, 
       t1.tweet_sentiment 
from tweet t1
inner join (select max(tweet_sentiment) as tweet_sentiment from tweet) t2 on t1.tweet_sentiment=t2.tweet_sentiment 
inner join (select location, max(tweet_sentiment) as tweet_sentiment, count(*) as count_location from tweet group by location) t3 on t1.location=t3.location
where trim(t3.location)is not null 
order by t1.tweet_sentiment desc
;

select location, max(tweet_sentiment) as tweet_sentiment, count(*) as count_location from tweet group by location
order by count(*) desc
;

drop table [place];
create table Place as
       select id, location from tweet where location is not null;       

alter table tweet add column place_id(fk_place REFERENCES place(id) ON DELETE CASCADE);

CREATE TABLE place(fk_master REFERENCES master(mid) ON DELETE CASCADE ...);

create table if not exists Place(id integer primary key, id integer foreign key, location text);
insert into place(location)
       select distinct location from tweet;       
select * from place;


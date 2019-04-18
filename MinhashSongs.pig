REGISTER bigdata.jar;
all_data = LOAD 'songdata_tabbed.csv' USING PigStorage('\t') as (singer:chararray, song:chararray, lyrics:chararray);
min_hashed = FOREACH all_data GENERATE CONCAT(singer, '|', song), kz.sdu.bdt.pig.MinHasher(lyrics) as hashes;

dump min_hashed;

REGISTER bigdata.jar;
all_data = LOAD 'songdata_tabbed.csv' USING PigStorage('\t') as (singer:chararray, song:chararray, lyrics:chararray);
tokenized = FOREACH all_data GENERATE singer, kz.sdu.bdt.pig.Tokenizer(lyrics) as tokens;
flat = foreach tokenized generate singer, flatten(tokens);
grp = group flat by ($0, $1);
cnt = foreach grp generate flatten(group) as (singer, word), COUNT($1);

g = group cnt by singer;
result = foreach g {
    prods = order cnt by $2 desc;
    top_prods = limit prods 1;
    generate flatten(top_prods);
};
--sorted = order result by $0;

dump result;

REGISTER bigdata.jar;
all_data = LOAD 'song_hashes' USING PigStorage('\t') as (song:chararray, hashes:chararray);
all_data2 = FOREACH all_data GENERATE song as song2, hashes as hashes2;
crossed = CROSS all_data, all_data2;
jac_sim = FOREACH crossed GENERATE song, song2, kz.sdu.bdt.pig.JaccardSimilarity(hashes, hashes2) as jaccard_similarity:double;
result = FILTER jac_sim BY song != song2 AND jaccard_similarity > 0.7;
dump result;

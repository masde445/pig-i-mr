ratingsData = LOAD '/home/sshuser/Chudy/ratings.csv' USING PigStorage(',') AS (userId:long, movieId:long, rating:double, timestamp:long);
dataFormatted = FOREACH ratingsData GENERATE movieId, rating;
dataFiltered = FILTER dataFormatted BY movieId > 0;
result = GROUP dataFiltered BY movieId;
resultFinal = FOREACH result GENERATE group,AVG(dataFiltered.rating);
DUMP resultFinal;
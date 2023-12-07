UPDATE genres
SET genre = TRIM(genre);

UPDATE stores
SET store = TRIM(store);

UPDATE platforms
SET platform = TRIM(platform);

SELECT esrb_rating, COUNT(*) 
FROM rawg_game_data
GROUP BY esrb_rating
ORDER BY COUNT(*) desc;

SELECT DISTINCT genre, COUNT(*)
FROM genres
GROUP BY DISTINCT genre;

SELECT r.name, r.released, ARRAY_TO_STRING(ARRAY_AGG(g.genre), ', ') as genres
FROM rawg_game_data r
RIGHT JOIN genres g ON r.name = g.name
GROUP BY r.name, r.released;

SELECT r.name, r.released, ARRAY_TO_STRING(ARRAY_AGG(p.platform), ', ') as platforms
FROM rawg_game_data r
RIGHT JOIN platforms p ON r.name = p.name
GROUP BY r.name, r.released;

SELECT name, array_to_string(array_agg(genre), ', ') as genre
FROM genres
GROUP BY name;

SELECT name, rating || ' / ' || rating_top as rating, ratings_count
FROM rawg_game_data
WHERE rating != 0
ORDER BY ratings_count desc;

SELECT name, released, metacritic
FROM rawg_game_data
WHERE metacritic IS NOT NULL
ORDER BY metacritic desc;

SELECT SUM(CASE WHEN metacritic >= 90 THEN 1 ELSE 0 END) as metacritic_more_than_90
FROM rawg_game_data;

SELECT name, released, metacritic
FROM rawg_game_data
WHERE metacritic >= 90
ORDER BY metacritic desc;

SELECT DISTINCT platform, COUNT(*) as total_games
FROM platforms
GROUP BY DISTINCT platform
ORDER BY COUNT(*) desc;

SELECT name, exceptional_rating_count, recommended_rating_count, meh_rating_count, skip_rating_count
FROM rawg_game_data
WHERE exceptional_rating_count != 0 OR recommended_rating_count != 0 OR meh_rating_count != 0
OR skip_rating_count != 0
ORDER BY exceptional_rating_count desc;

WITH CTE AS (SELECT name, EXTRACT(YEAR FROM released) as year, rating, DENSE_RANK() OVER(PARTITION BY EXTRACT(YEAR FROM released) ORDER BY rating desc) as rank_num
FROM rawg_game_data
WHERE rating != 0)
SELECT name, year, rating
FROM CTE
WHERE rank_num = 1
;



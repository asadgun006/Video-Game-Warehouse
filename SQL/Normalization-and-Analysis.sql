--This procedure creates additional tables for normalization and better analysis
--A new table is created from the base table saved by the pipeline for backup
--The procedure does type casting, deletes duplicate rows and creates new 
--tables for game genres, platforms, and stores, with game name being the foreign
--key in each table

CREATE OR REPLACE PROCEDURE public.explode_columns(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	CREATE TABLE rawg_game_data AS (SELECT * FROM rawg_extracted_data);
	
	ALTER TABLE rawg_game_data
	ALTER COLUMN released TYPE DATE USING released::date;
	
	DELETE FROM rawg_game_data
	WHERE unique_row_id IN (SELECT unique_row_id FROM (SELECT unique_row_id, ROW_NUMBER() OVER(PARTITION BY name ORDER BY released) as row_num 
								 FROM rawg_game_data) rd WHERE rd.row_num > 1);
								 
	CREATE TABLE genres AS (SELECT name, UNNEST(STRING_TO_ARRAY(genres, ',')) as genre 
							FROM rawg_game_data);
							
	CREATE TABLE platforms AS (SELECT name, UNNEST(STRING_TO_ARRAY(platforms, ',')) as platform
							  FROM rawg_game_data);
	CREATE TABLE stores as (SELECT name, UNNEST(STRING_TO_ARRAY(stores, ',')) as store 
							FROM rawg_game_data);
							
	ALTER TABLE rawg_game_data
	DROP COLUMN stores, DROP COLUMN platforms, DROP COLUMN genres;
END;
$BODY$;

--These update statements remove trailing spaces from respective columns
--to eliminate duplication
UPDATE genres
SET genre = TRIM(genre);

UPDATE stores
SET store = TRIM(store);

UPDATE platforms
SET platform = TRIM(platform);

-- The number of games for each esrb_rating
SELECT esrb_rating, COUNT(*) 
FROM rawg_game_data
GROUP BY esrb_rating
ORDER BY COUNT(*) desc;

-- Number of games for each genre
SELECT DISTINCT genre, COUNT(*)
FROM genres
GROUP BY DISTINCT genre;

-- Name, release date, and genres of each game as a comma separated text string where 
-- each game has at least one genre
SELECT r.name, r.released, ARRAY_TO_STRING(ARRAY_AGG(g.genre), ', ') as genres
FROM rawg_game_data r
RIGHT JOIN genres g ON r.name = g.name
GROUP BY r.name, r.released;

-- Name, release date, and platforms of each game as a comma separated text string where 
-- each game has at least one platform
SELECT r.name, r.released, ARRAY_TO_STRING(ARRAY_AGG(p.platform), ', ') as platforms
FROM rawg_game_data r
RIGHT JOIN platforms p ON r.name = p.name
GROUP BY r.name, r.released;

-- Genres of each game with a valid genre
SELECT name, array_to_string(array_agg(genre), ', ') as genre
FROM genres
GROUP BY name;

-- Rating of each game that does not have a 0 rating, ordered by highest first
SELECT name, rating || ' / ' || rating_top as rating, ratings_count
FROM rawg_game_data
WHERE rating != 0
ORDER BY ratings_count desc;

-- Name, release date, and Metacritic rating of each game
SELECT name, released, metacritic
FROM rawg_game_data
WHERE metacritic IS NOT NULL
ORDER BY metacritic desc;

-- Number of games with metacritic rating from 90 - 100
SELECT SUM(CASE WHEN metacritic >= 90 THEN 1 ELSE 0 END) as metacritic_more_than_90
FROM rawg_game_data;

-- Games with metacritic greater than 90
SELECT name, released, metacritic
FROM rawg_game_data
WHERE metacritic >= 90
ORDER BY metacritic desc;

-- Number of games released for each platform
SELECT DISTINCT platform, COUNT(*) as total_games
FROM platforms
GROUP BY DISTINCT platform
ORDER BY COUNT(*) desc;

-- Number of respective rating for each game, ordered by highest number of exceptional ratings
SELECT name, exceptional_rating_count, recommended_rating_count, meh_rating_count, skip_rating_count
FROM rawg_game_data
WHERE exceptional_rating_count != 0 OR recommended_rating_count != 0 OR meh_rating_count != 0
OR skip_rating_count != 0
ORDER BY exceptional_rating_count desc;

-- Highest rated games for each year between 2000 and 2022
WITH CTE AS (SELECT name, EXTRACT(YEAR FROM released) as year, rating, DENSE_RANK() OVER(PARTITION BY EXTRACT(YEAR FROM released) ORDER BY rating desc) as rank_num
FROM rawg_game_data
WHERE rating != 0)
SELECT name, year, rating
FROM CTE
WHERE rank_num = 1
;



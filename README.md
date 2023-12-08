# Video-Game-Warehouse
A Video Game Warehouse for 461,874 games released on PC, PlayStation, and Xbox (Not exclusively) from 2000 - 2022. From RAWG API.

I wanted to get hands-on with Apache Airflow and Docker, and decided to make a video games database using RAWG API. 
## Resources Used
1. `Apache Airflow:` Used for pipeline orchestration. Running on WSL
2. `Docker Desktop:` Used for hosting the Airflow server and running the DAG using Docker compose and Airflow custom image
3. `Azure Database for PostgreSQL flexible server:` Database storage
4. `PgAdmin 4:` For database normalization and SQL analysis

Additional instructions can be found in `airflow-docker`

## Data available
The data is divided into four tables: 
* `rawg_game_data` Main table with majority of the details
* `genres` Different row for each individual genre. Does not contain games with no genre listed.
* `platforms` Different row for each individual platform.
* `stores` Different row for each individual store

### rawg_game_data
* `name:` Name of the game
* `playtime:` Number of hours to complete the game
* `released:` Release date of the game
* `rating:` Rating of the game
* `rating_top:` Maximum rating
* `rating_count:` Number of ratings on RAWG
* `metacritic:` Metacritic rating for the game
* `updated:` Last updated date by RAWG
* `id:` Unique ID 
* `esrb_rating:` ESRB rating
* `reviews_count:` Number of reviews on RAWG
* `exceptional_rating_count:` Number of users who rated this game as 'Exceptional'
* `recommended_rating_count:` Number of users who rated this game as 'Recommended'
* `meh_rating_count:` Number of users who rated this game as 'Meh'
* `skip_rating_count:` Number of users who rated this game as 'Skip'
* `unique_row_id:` Database table identifier

### genres 
* `name:` Name of the game
* `genre:` Genre of the game

### platforms 
* `name:` Name of the game
* `platform:` Platform the game was released on

### stores 
* `name:` Name of the game
* `store:` Store the game was released on

## Contact
Please let me know about any queries or suggestions on my email :) `asadgundra60@gmail.com`







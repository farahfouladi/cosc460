/* COSC 460 Fall 2014, Lab 3 */

/* These set the output format.  Please be sure to leave these settings as is. */
.header OFF
.mode list 

/* For each of the queries below, put your SQL in the place indicated by the comment.  
   Be sure to have all the requested columns in your answer, in the order they are 
   listed in the question - and be sure to sort things where the question requires 
   them to be sorted, and eliminate duplicates where the question requires that.   
   I will grade the assignment by running the queries on a test database and 
   eyeballing the SQL queries where necessary.  I won't grade on SQL style, but 
   we also won't give partial credit for any individual question - so you should be 
   confident that your query works. In particular, your output should match 
   the example output.
*/

/* Q1 -  Find the titles of all movies directed by Steven Spielberg.  */
select " ";
select "Q1";

/* Put your SQL for Q1 below */
select * from Movie where director = "Steven Spielberg";

/* Q2 -  Find all years that have a movie that received a rating of 4 or 5, 
         and sort them in increasing order.             
*/
select " ";
select "Q2";

/* Put your SQL for Q2 below */
select distinct year from Movie, Rating where Movie.mID = Rating.mID and stars>=4 order by year;

/* Q3 -  Find the titles of all movies that have no ratings.
*/
select " ";
select "Q3";

/* Put your SQL for Q3 below */
select title from Movie where mID not in (select mID from Rating);

/* Q4 -  Write a query to return the ratings data in a more 
         readable format: reviewer name, movie title, stars, and ratingDate. 
         Also, sort the data, first by reviewer name, then by movie title, 
         and lastly by number of stars, all in ascending order.
*/
select " ";
select "Q4";

/* Put your SQL for Q4 below */
select Rev.name, M.title, Rate.stars, Rate.ratingDate from Movie M, Reviewer Rev, Rating Rate where Rev.rID = Rate.rID and M.mID = Rate.mID order by Rev.name, M.title, Rate.stars;

/* Q5 -  For all cases where the same reviewer rated the same movie twice 
         and gave it a higher rating the second time, return the reviewer's 
         name and the title of the movie.
*/
select " ";
select "Q5";

/* Put your SQL for Q5 below */
select Rev.name, M.title from Movie M, Reviewer Rev, Rating Rate, Rating Rate2 where M.mID = Rate.mID and Rev.rID = Rate.rID and Rate.rID = Rate2.rID and Rate.mID = Rate2.mID and Rate.ratingDate < Rate2.ratingDate and Rate.stars < Rate2.stars;



/* Q6 - For each movie that has at least one rating, find the highest number 
        of stars that movie received. Return the movie title and number of 
        stars. Sort by movie title. 
*/
select " ";
select "Q6";

/* Put your SQL for Q6 below */
select M.title, max(Rate.stars) from Movie M, Rating Rate where M.mID = Rate.mID group by M.title order by M.title;

/* Q7 - For each movie, the title along with the number of ratings it has 
        received.  Your result should include those movies that have zero ratings.                                                                 
*/
select " ";
select "Q7";

/* Put your SQL for Q7 below */
Select title from Movie M, Rating Rate where M.mID = Rate.mID union Select title from Movie M, Rating Rate where M.mID != Rate.mID;

/* Q8 - For each movie that has at least one rating, return the title and the 
        'rating spread', that is, the difference between highest and lowest 
        ratings given to that movie. Sort by rating spread from highest to 
        lowest, then by movie title alphabetically.   
*/
select " ";
select "Q8";

/* Put your SQL for Q8 below */
select M.title, max(Rate.stars) - min(Rate.stars) as RatingSpread from Movie M, Rating Rate where M.mID = Rate.mID group by M.title order by RatingSpread desc, M.title;


/* Q9 -  Find the difference between the average rating of movies released before 
         1980 and the average rating of movies released after 1980. (Make sure to 
         calculate the average rating for each movie, then the average of those 
         averages for movies before 1980 and movies after. Don't just calculate 
         the overall average rating before and after 1980.)  
*/
select " ";
select "Q9";

/* Put your SQL for Q9 below */
select avg(before1980.before1980) - avg(after1980.after1980) from (select avg(Rate.stars) as before1980 from Movie M, Rating Rate where M.mID = Rate.mID and M.year < 1980 group by M.title) before1980, (select avg(Rate.stars) as after1980 from Movie M, Rating Rate where M.mID = Rate.mID and M.year > 1980 group by M.title) after1980;


/* Q10 - For each director, return the director's name together with the title(s) 
         of the movie(s) they directed that received the highest rating among all 
         of their movies, and the value of that rating. 
*/
select " ";
select "Q10";

/* Put your SQL for Q10 below */
select distinct director, title, stars from (Movie join Rating using(mID)) M where stars in (select max(stars) from Rating join Movie using (mID) where M.director = director);













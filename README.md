# Recommendation-Syastem
For Task-2: I am doing Item based recommendation filtering as well as content based collaborative
filtering for better results. For finding weights of similarities, I have implemented Pearson Correlation.

1. MISSING VALUE AND OUTLIERS PREDICTED RATINGS
  • Missing Values – cold start problem:
  when we predict the rating for (user_id,movie_id) we have to see if both are present in the
  training file:
  1. If <user_id> is missing:
  we will assign average rating for that particular movie_Id(simple mean imputation)
  2. If <movie_id> is missing:
  We will assign the average of all the ratings given to all the items rated by that user (simple
  mean imputation)
  3. If both are missing:
  if both are not present then we give a rating of 2.5 to be more close to get the minimum error.

2. IMPROVEMENT TO THE RECOMMENDATION SYSTEM
  ➔ CASE AMPLIFICATION :
  • Transform applied to the weights calculated in Item-based recommendation system
  • This reduces noise in the data
  • Higher values are favored
  • I have considered rho=2.5
  
## HOW TO RUN PROGRAMS
• TASK 1 :
spark-submit --class "Dhanashree_Naik_task1" --master local[*] <path_for_jar>
<path_for_ratings.csv> <path_for_testing_small.csv>
• TASK 2 :
spark-submit --class "Dhanashree_Naik_task2" --master local[*] <path_for_jar>
<path_for_ratings.csv> <path_for_testing_small.csv>
• Arguments passed are in the following order:
1. path to ratings.csv
2. path to testing_small.csv

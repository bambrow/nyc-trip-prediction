# Travel Duration Prediction of Taxi and Bike Trips in New York City

## Introduction
Can we predict the trip duration of taxi and bike trips in New York City? Which features have the greatest impact upon the trip duration? To study this problem, we collected the weather data, taxi trip data, and CitiBike trip data in New York City and utilized machine learning algorithms with several optimizations to estimate the trip duration in New York City.   All the data was put in HDFS for our analyzation and application programming, using Spark with Scala. Weather data was joined to the trip data in order to find out how much effect weather conditions will cause to the trip durations. Mainly three machine learning regression algorithms in Spark MLlib were used - Linear Regression, Decision Tree and Random Forest, with several features carefully selected and constructed based on the profiled data. Trip distances were calculated based upon the coordinates provided by the datasets, by converting the coordinates to cartesian coordinates and calculating the Euclidean distances, to be provided in our prediction model for better accuracy; k-means clustering was also used to improve the prediction precision by clustering the starting point and ending point coordinates to a reasonable number of cluster centers. The performance of these three machine learning algorithms are evaluated and compared, and our final model was proved to reduce the mean square error (MSE) by over 50%. For the convenience of users to use our model, an application with unified interface was built, so users can use our optimized algorithm for training and testing, based upon their own selection of data and machine learning algorithm.

## Project Contributors
- Weiqiang Li
- Liheng Gong

## Motivation
For travel methods like taxis and bikes, we study whether we can use features like location coordinates, time and weather conditions to predict trip time. We also analyze which features have the most impact on the prediction of trip durations. If the predictions are reasonable, then we can use historical trip and weather data to make reliable predictions of trip duration if location, time, weather and other required features are provided. We can also predict future trips based upon the starting and ending points together with weather forecast and other important features. Commuters using taxis and bikes can utilize the predictions to better schedule commute plans and taxi/bike service providers can also use the prediction to optimize taxi/bike dispatching and improve service quality.

## Project Report
The detailed project report can be found [here](https://github.com/bambrow/nyc-trip-prediction/blob/master/project_report_github.pdf).

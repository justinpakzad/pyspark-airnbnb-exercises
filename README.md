# PySpark Airbnb Exercises
To get more familiar with PySpark, I had chat-GPT generate some sample exercises based on the Airbnb dataset, which you can find [here](https://insideairbnb.com/get-the-data/). There are various tasks here involving cleaning, transforming, and analyzing the dataset. I chose to use data from New York City, Los Angeles,San Francisco, and Chicago; if you want to give them a go, you can choose data from any city/cities you are interested in. For extra practice, push the reports directly to an S3 bucket instead of saving them locally to disk.

## Project Structure
- `src/` - 
  - `analytics.py` - Contains analytics and aggregation logic
  - `main.py` - Main entry point for running the Spark application
  - `schemas.py` - Schemas for the input data
  - `transform.py` - Data transformation and cleaning logic
  - `utils.py` - Utility functions 
- `tests/` - 
  - `test_transformations.py` - Unit tests for transformations

## Setup
1. Clone the repository:
```
git clone https://github.com/justinpakzad/pyspark-airnbnb-exercises
```
2. Install required Python dependencies:
```
pip install -r requirements.txt
```

## Examples
1. Run Tests
```
pytest test_transformations.py
```
2. Generate Reports
```
python main.py
```

## Exercises

### 1. Load and Prepare Data
- Load the datasets into PySpark DataFrames using the provided schema.
- Handle missing values, remove invalid data, and ensure consistency across the datasets.
- Convert relevant columns to appropriate data types for analysis.



### 2. Top 5 Listings by Average Price Per Month
- **Objective:** Identify the top 5 listings with the highest average price for each month. Use monthly review counts as a tiebreaker for listings with the same price.
- **Output:** Save a report with listing IDs, neighborhoods, average price, review counts, and month.



### 3. Listings with Highest Occupancy Rates by Month
- **Objective:** Calculate the occupancy rate for each listing per month and identify the top 10 listings with the highest rates. Include relevant details such as listing IDs, neighborhoods, and monthly revenue.
- **Output:** Save a report showing the top 10 listings per month by occupancy rate.



### 4. Most Reviewed Listings by Month
- **Objective:** Find the 10 most reviewed listings for each month. Exclude listings with fewer than 20 reviews to avoid outliers.
- **Output:** Save a report showing listing IDs, review counts, neighborhoods, and month.



### 5. Top 5 Hosts by Total Listings and Average Price
- **Objective:** Identify the top 5 hosts with the most properties and calculate their average price across all listings.
- **Output:** Save a report listing host IDs, the number of properties they manage, and their average price.



### 6. Neighborhoods with the Most Active Hosts
- **Objective:** Identify the neighborhoods with the most active hosts by counting the total number of hosts in each neighborhood. Include additional details such as the average number of properties managed per host and the percentage of superhosts in each neighborhood.
- **Output:** Save a report listing neighborhoods, total hosts, average properties per host, and percentage of superhosts.



### 7. Monthly Revenue by Neighborhood
- **Objective:** Calculate the total monthly revenue for each neighborhood and how much the monthly revenues deviate from the yearly average revenue (variance).
- **Output:** Save a report that includes the monthly revenues for each neighborhood and their variance over the year.



### 8. Listings with Lowest Occupancy Rates and Review Scores
- **Objective:** Identify the 10 listings with the lowest occupancy rates and lowest review scores, but only include listings with at least 10 reviews.
- **Output:** Save a report showing listing IDs, neighborhoods, occupancy rates, and review scores.



### 9. Review Distribution by Neighborhood
- **Objective:** Analyze how reviews are distributed across neighborhoods, identifying areas with the most active user feedback.
- **Output:** Save a report showing neighborhoods ranked by their review counts.



### 10. Neighborhood Amenity Clusters and Price Analysis
- **Objective:** Calculate the average number of amenities per listing for each neighborhood, cluster neighborhoods into three groups (Low, Medium, and High amenities), and analyze the average price within each cluster.
- **Output:** Save a report listing the neighborhood cluster (Low, Medium, or High), the number of neighborhoods in the cluster, the average price for listings in each cluster, and the price difference between clusters.



### 11. Room Type Price Trends Across Seasons
- **Objective:** Analyze how the average prices for different room types vary across the seasons. Determine whether certain room types are more affected by seasonal changes than others.
- **Output:** Save a report with columns for room type, season, average price, and seasonal price difference (the difference between the highest and lowest average prices across the seasons for that room type).



### 12. Hosts with the Most Types of Listings
- **Objective:** Identify the top 5 hosts with the most diverse types of property listings.
- **Output:** Report listing host IDs and count of each type of property they manage.



### 13. Superhost Performance Analysis
- **Objective:** Compare the average occupancy rates, prices, and review scores of superhosts versus non-superhosts.
- **Output:** Save a report summarizing performance metrics for superhosts and non-superhosts.



### 14. Host Response Time and Booking Rates
- **Objective:** Analyze how host response times correlate with occupancy rates and reviews.
- **Output:** Save a report showing response time categories, average occupancy rates, review scores, and review counts.



### 15. Price Range Dynamics and Occupancy Rates
- **Objective:** Bin listings into price ranges (e.g., Low, Medium, High) based on their average prices and analyze the occupancy rates and review scores within each bin. Identify whether listings in certain price ranges consistently perform better.
- **Output:** Save a report listing price bins, the number of listings in each bin, their average occupancy rates, and average review scores.



### 16. Daily Price Changes
- **Objective:** Identify listings that actively utilize dynamic pricing strategies. For each listing, calculate the percentage of days in the year where the price changes compared to the previous day. Rank the top 5 listings that exhibit the most frequent price changes.

- **Output:** Save a report showing listing IDs, neighborhoods, total number of price changes, percentage of days with price changes, and average price.



### 17. Effect of Host Identity Verification on Booking Performance  
- **Objective:** Evaluate how host identity verification status impacts booking performance, including proxy booking volume (since we don't directly have booking data), occupancy rates,revenue, and review scores for their listings.

- **Output:** Save a report showing host IDs, their verification status, total bookings per month, average monthly occupancy rate, and average monthly revenue.




### 18. Host Tier Analysis and Revenue Trends  
- **Objective:** Analyze the performance of hosts by categorizing them into tiers based on the total number of listings they manage (e.g., Small: 1-5, Medium: 6-15, Large: 16+). Evaluate how these tiers correlate with total revenue, review scores, and listing diversity (number of room types and neighborhoods). 

- **Output:** Save a report showing host IDs, host tier (Small, Medium, Large), total listings managed, total revenue, average review score, average occupancy rate, number of unique room types, number of unique neighborhoods, and rank within tier based on total revenue and total properties.  



### 19. Top Revenue-Contributing Neighborhood Groups  
- **Objective:** Rank neighborhood groups by their total revenue contribution and identify the top-performing listings within each group. Analyze how listing diversity (room types and property types) and occupancy rates contribute to revenue variations across groups. Calculate average review scores, and evaluate the impact of high-revenue listings on group performance.  
- **Output:** Save a report showing neighborhood groups, total revenue, average review scores, listing diversity (number of unique room types and property types), average occupancy rates, and average review scores.


### 20. Impact of Property Type on Pricing Trends  
- **Objective:** Compare average prices across different property types and evaluate their relationship with review scores and occupancy rates. Highlight property types with the highest price-to-performance ratio and rank them by this metric to identify top-performing property types.  
- **Output:** Save a report showing property types, average prices, average review scores, average occupancy rates, price-to-performance ratio (average price / performance score (performance score = average review score × 0.6 + average occupancy rate × 0.4)), and rank based on price-to-performance ratio.  



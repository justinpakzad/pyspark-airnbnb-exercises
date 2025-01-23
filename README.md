# PySpark Airbnb Exercises
To get more familiar with PySpark, I had chat-GPT generate some sample exercises based on the Airbnb dataset, which you can find [here](https://insideairbnb.com/get-the-data/). There are various tasks here involving cleaning, transforming, and analyzing the dataset. I chose to use data from New York City, Los Angeles,San Francisco, and Chicago; if you want to give them a go, you can choose data from any city/cities you are interested in.

## Setup
1. Clone the repository:
```
git clone ----
```
2. Install required Python dependencies:
```
pip install -r requirements.txt
```
## Examples
1. Run Tests
```
pytest unit_test.py
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

---

### 2. Top 5 Listings by Average Price Per Month
- **Objective:** Identify the top 5 listings with the highest average price for each month. Use total review counts as a tiebreaker for listings with the same price.
- **Output:** Save a report with listing IDs, neighborhoods, average price, review counts, and month.

---

### 3. Listings with Highest Occupancy Rates by Month
- **Objective:** Calculate the occupancy rate for each listing per month and identify the top 10 listings with the highest rates. Include relevant details such as listing IDs, neighborhoods, and monthly revenue.
- **Output:** Save a report showing the top 10 listings per month by occupancy rate.

---

### 4. Most Reviewed Listings by Month
- **Objective:** Find the 10 most reviewed listings for each month. Exclude listings with fewer than 20 reviews to avoid outliers.
- **Output:** Save a report showing listing IDs, review counts, neighborhoods, and month.

---

### 5. Top 5 Hosts by Total Listings and Average Price
- **Objective:** Identify the top 5 hosts with the most properties and calculate their average price across all listings.
- **Output:** Save a report listing host IDs, the number of properties they manage, and their average price.

---

### 6. Neighborhoods with the Most Active Hosts
- **Objective:** Identify the neighborhoods with the most active hosts by counting the total number of hosts in each neighborhood. Include additional details such as the average number of properties managed per host and the percentage of superhosts in each neighborhood.
- **Output:** Save a report listing neighborhoods, total hosts, average properties per host, and percentage of superhosts.

---

### 7. Monthly Revenue by Neighborhood
- **Objective:** Calculate the total monthly revenue for each neighborhood and how much the monthly revenues deviate from the yearly average revenue (variance).
- **Output:** Save a report that includes the monthly revenues for each neighborhood and their variance over the year.

---

### 8. Listings with Lowest Occupancy Rates and Review Scores
- **Objective:** Identify the 10 listings with the lowest occupancy rates and lowest review scores, but only include listings with at least 10 reviews.
- **Output:** Save a report showing listing IDs, neighborhoods, occupancy rates, and review scores.

---

### 9. Review Distribution by Neighborhood
- **Objective:** Analyze how reviews are distributed across neighborhoods, identifying areas with the most active user feedback.
- **Output:** Save a report showing neighborhoods ranked by their review counts.

---

### 10. Neighborhood Amenity Clusters and Price Analysis
- **Objective:** Calculate the average number of amenities per listing for each neighborhood, cluster neighborhoods into three groups (Low, Medium, and High amenities), and analyze the average price within each cluster.
- **Output:** Save a report listing the neighborhood cluster (Low, Medium, or High), the number of neighborhoods in the cluster, the average price for listings in each cluster, and the price difference between clusters.

---

### 11. Room Type Price Trends Across Seasons
- **Objective:** Analyze how the average prices for different room types vary across the seasons. Determine whether certain room types are more affected by seasonal changes than others.
- **Output:** Save a report with columns for room type, season, average price, and seasonal price difference (the difference between the highest and lowest average prices across the seasons for that room type).

---

### 12. Hosts with the Most Types of Listings
- **Objective:** Identify the top 5 hosts with the most diverse types of property listings.
- **Output:** Report listing host IDs and count of each type of property they manage.

---

### 13. Superhost Performance Analysis
- **Objective:** Compare the average occupancy rates, prices, and review scores of superhosts versus non-superhosts.
- **Output:** Save a report summarizing performance metrics for superhosts and non-superhosts.

---

### 14. Host Response Time and Booking Rates
- **Objective:** Analyze how host response times correlate with occupancy rates and reviews.
- **Output:** Save a report showing response time categories, average occupancy rates, review scores, and review counts.

---

### 15. Price Range Dynamics and Occupancy Rates
- **Objective:** Bin listings into price ranges (e.g., Low, Medium, High) based on their average prices and analyze the occupancy rates and review scores within each bin. Identify whether listings in certain price ranges consistently perform better.
- **Output:** Save a report listing price bins, the number of listings in each bin, their average occupancy rates, and average review scores.

---

### 16. Variability of Average Prices Over Time by Listing
- **Objective:** Analyze how the average prices of listings vary over time. Identify listings with the most price variability month-over-month.
- **Output:** Save a report showing listing IDs, average monthly prices, and variance in price over the year.

---

### 17. Effect of Host Identity Verification on Booking Volume
- **Objective:** Evaluate how host identity verification status impacts the booking volume (number of bookings) for their listings.
- **Output:** Save a report showing host IDs, their verification status, and total bookings per month.

---

### 18. Correlation Between Review Scores and Host Profile Completeness
- **Objective:** Determine if there is a correlation between detailed host profiles (profile picture, verified identity) and their review scores.
- **Output:** Save a report showing host IDs, presence of profile picture, identity verification status, and average review scores.

---

### 19. Listing Density and Its Impact on Prices
- **Objective:** Investigate how the density of listings in a neighborhood (calculated as listings per square mile) affects the average listing prices.
- **Output:** Save a report showing neighborhoods, listing density, and average listing prices.

---

### 20. Booking Frequency of Instantly Bookable Listings
- **Objective:** Compare the booking frequency of instantly bookable listings versus those that are not instantly bookable across different room types.
- **Output:** Save a report showing room types, instant bookable status, and average bookings per month.

---

### 21. Longevity of Listings and Their Performance
- **Objective:** Examine how the longevity of a listing (time from first review to last review) relates to its total number of reviews and average review score.
- **Output:** Save a report listing IDs, time span from first to last review, total number of reviews, and average review score.

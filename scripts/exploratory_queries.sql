-- ✅ Check total number of reviews
SELECT COUNT(*) AS total_reviews FROM helpful_reviews;

-- ✅ Find top 10 most-reviewed products (Filtered for relevance)
SELECT asin, COUNT(*) AS review_count 
FROM helpful_reviews 
WHERE text IS NOT NULL AND LENGTH(text) > 20  -- Exclude empty reviews
GROUP BY asin 
ORDER BY review_count DESC 
LIMIT 10;

-- ✅ Compute average ratings per product (Avoid bias from low review counts)
SELECT asin, AVG(rating) AS avg_rating, COUNT(*) AS review_count 
FROM helpful_reviews 
GROUP BY asin 
HAVING review_count > 10  -- Corrected: Filtering after aggregation
ORDER BY avg_rating DESC 
LIMIT 10;

-- ✅ Most frequent words in review titles (Exclude common stopwords)
SELECT word, count 
FROM title_word_counts 
WHERE word NOT IN ('the', 'and', 'is', 'it', 'to', 'a', 'on', 'of', 'in')  -- Remove generic words
ORDER BY count DESC 
LIMIT 20;

-- ✅ Most frequent words in review text (Stopword optimization)
SELECT word, count 
FROM text_word_counts 
WHERE word NOT IN ('the', 'and', 'is', 'it', 'to', 'a', 'on', 'of', 'in')  
ORDER BY count DESC 
LIMIT 20;

-- ✅ Percentage of verified purchases (Efficient calculation)
SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM helpful_reviews WHERE verified_purchase IS NOT NULL)) AS verified_percentage 
FROM verified_reviews;

-- ✅ Find products with the highest percentage of helpful votes
SELECT asin, AVG(helpful_vote) AS avg_helpful_votes, COUNT(*) AS review_count 
FROM helpful_reviews 
WHERE helpful_vote > 0  -- Only consider reviews with helpful votes
GROUP BY asin 
ORDER BY avg_helpful_votes DESC 
LIMIT 10;

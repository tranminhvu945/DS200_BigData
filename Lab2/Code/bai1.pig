-- Load dữ liệu
reviews = LOAD '/lab2/input/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Chỉ giữ mỗi review duy nhất theo id + review
unique_reviews = FOREACH reviews GENERATE
    id,
    review;

unique_reviews = DISTINCT unique_reviews;

-- Đưa review về chữ thường, bỏ ký tự đặc biệt, gom khoảng trắng
clean_reviews = FOREACH unique_reviews GENERATE
    id,
    TRIM(
        REPLACE(
            REPLACE(
                LOWER(review),
                '[^\\p{L}\\p{N}\\s]', ' '
            ),
            '\\s+', ' '
        )
    ) AS clean_review;

-- Tách review thành các từ theo khoảng trắng
review_words = FOREACH clean_reviews GENERATE
    id,
    FLATTEN(TOKENIZE(clean_review)) AS word;

-- Loại token rỗng
review_words = FILTER review_words BY word IS NOT NULL AND word != '';

-- Load stopword
stopwords_raw = LOAD '/lab2/input/stopwords.txt' USING PigStorage('\n') AS (stopword: chararray);

-- Chỉ bỏ ký tự CR do file Windows
stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

-- LEFT OUTER JOIN để đối chiếu stopword
joined_data = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;

-- Giữ lại những từ không nằm trong stopword
filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

-- Kết quả cuối
result = FOREACH filtered_words GENERATE
    review_words::id   AS id,
    review_words::word AS word;

-- Lưu kết quả
STORE result INTO '/lab2/output/bai1/' USING PigStorage('\t');